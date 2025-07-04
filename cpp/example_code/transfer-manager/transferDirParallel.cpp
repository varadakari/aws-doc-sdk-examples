#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include "ThreadPool.h"

class FileStream : public Aws::IOStream {
public:
    explicit FileStream(const std::string& path, std::ios::openmode mode = std::ios::in | std::ios::binary)
        : Aws::IOStream(&filebuf_), filebuf_() {
        filebuf_.open(path.c_str(), std::ios::in | std::ios::binary);
        if (!filebuf_.is_open()) {
            throw std::runtime_error("Failed to open file: " + path);
        }
    }

    ~FileStream() {
        filebuf_.close();
    }

private:
    std::filebuf filebuf_;
};

int UploadFile(std::shared_ptr<Aws::Transfer::TransferManager> transferManager,
               const std::string& file_path,
               const std::string& bucket,
               const std::string& s3_key) {

    struct stat st;
    if (stat(file_path.c_str(), &st) != 0) {
        std::cerr << "File stat failed\n";
        return 1;
    }

    std::cout << " file_path: " << file_path << " bucket: " << bucket <<" s3_key: " << s3_key << std::endl;
    auto stream = Aws::MakeShared<FileStream>("UploadTag", file_path);
    auto handle = transferManager->UploadFile(
                        stream,
                        bucket,
                        s3_key,
                        "application/octet-stream",
                        Aws::Map<Aws::String, Aws::String>(),
                        Aws::MakeShared<Aws::Client::AsyncCallerContext>("upload_ctx")
                    );

    handle->WaitUntilFinished();

    if (handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED) {
        //std::cout << "Uploaded: " << s3_key << std::endl;
    } else {
        std::cerr << "Failed: " << s3_key
                  << " Error: " << handle->GetLastError().GetMessage() << std::endl;
        return 1;
    }
    return 0;
}

// Traverse directory and dispatch uploads
void process_directory(const std::string& dir_path,
                       const std::string& bucket_name,
                       ThreadPool& pool,
		       std::shared_ptr<Aws::Transfer::TransferManager> transferManager,
                       size_t high_watermark = 100000,
                       size_t low_watermark = 20000) {

    std::vector<std::string> files;

    DIR* dir = opendir(dir_path.c_str());
    if (!dir) {
        std::cerr << "Failed to open directory: " << dir_path << std::endl;
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        std::string name(entry->d_name);
        if (name == "." || name == "..") continue;

        std::string full_path = dir_path + "/" + name;

        struct stat st;
        if (stat(full_path.c_str(), &st) == -1) {
            std::cerr << "Could not stat: " << full_path << std::endl;
            continue;
        }

        if (S_ISREG(st.st_mode)) {
            // Bounded queue control
            while (pool.getQueueSize() >= high_watermark) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                while (pool.getQueueSize() > low_watermark) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }

            pool.enqueue([&transferManager, full_path, bucket_name, name]() {
                UploadFile(transferManager, full_path, bucket_name, name);
            });
        }
    }

    closedir(dir);
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <directory_path> <bucket_name> <num_threads>\n";
        return 1;
    }

    const std::string dirPath = argv[1];
    const std::string bucket = argv[2];
    int num_threads = std::stoi(argv[3]);
    size_t max_queue_size = 100000;


    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
	Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true;
        config.endpointOverride = "https://10.10.10.155:9000";
        config.region = "us-east-1";
        const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";
        config.requestTimeoutMs = 60000;   // 30 seconds or higher as needed
        config.connectTimeoutMs = 10000;   // connection timeout
        //config.httpLibOverride = Aws::Http::TransferLibType::CURL_CLIENT;
        config.maxConnections = 64;  // Match thread pool size

        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;

        std::shared_ptr<Aws::S3::S3Client> s3Client = std::make_shared<Aws::S3::S3Client>(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()), config, signPayloads, false);

        //auto executor = Aws::MakeShared<Aws::Utils::Threading::DefaultExecutor>("TransferManagerExecutor");
	auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("ExecutorTag", 16);
        Aws::Transfer::TransferManagerConfiguration transfer_config(executor.get());
        transfer_config.s3Client = s3Client;
        transfer_config.transferBufferMaxHeapSize = 64 * 1024 * 1024;  // 64 MB
        transfer_config.bufferSize = 8 * 1024 * 1024;                  // 8 MB per buffer

        auto transferManager = Aws::Transfer::TransferManager::Create(transfer_config);
	ThreadPool pool(num_threads, max_queue_size);
        process_directory(dirPath, bucket, pool, transferManager);
        pool.shutdown();  // Wait for all uploads to finish

    }
    Aws::ShutdownAPI(options);
    return 0;
}

