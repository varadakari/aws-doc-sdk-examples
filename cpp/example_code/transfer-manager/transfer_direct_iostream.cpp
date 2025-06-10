#include <aws/core/Aws.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h> // For Aws::FStream
#include <aws/core/utils/memory/stl/AWSAllocator.h> // For Aws::Allocator
#include <fstream>
#include <aws/transfer/TransferManager.h>
#include <dirent.h>
#include <fstream>
#include <memory>
#include <thread>
#include <future>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


class DirectIOStreamBuf : public std::streambuf {
public:
    DirectIOStreamBuf(const std::string& path, size_t chunk_size = 2 *1024 * 1024)
        : fd_(-1), file_size_(0), read_offset_(0) {

        // Align chunk size to 4096
        chunk_size_ = ((chunk_size + 4095) / 4096) * 4096;

        if (posix_memalign(reinterpret_cast<void**>(&buffer_), 4096, chunk_size_) != 0) {
            throw std::runtime_error("Failed to allocate aligned buffer");
        }

        fd_ = open(path.c_str(), O_RDONLY | O_DIRECT);
        if (fd_ < 0) {
            free(buffer_);
            throw std::runtime_error("Failed to open file: " + path);
        }

        struct stat st;
        if (fstat(fd_, &st) == 0) {
            file_size_ = st.st_size;
        }

        setg(buffer_, buffer_ + chunk_size_, buffer_ + chunk_size_); // initially empty
    }

    ~DirectIOStreamBuf() override {
        if (fd_ >= 0) close(fd_);
        free(buffer_);
    }

    size_t GetFileSize() const { return file_size_; }

protected:
    int_type underflow() override {
        if (read_offset_ >= file_size_) {
            return traits_type::eof();
        }

        size_t to_read = std::min(chunk_size_, file_size_ - read_offset_);
        size_t aligned_read = ((to_read + 4095) / 4096) * 4096;
        std::memset(buffer_, 0, aligned_read);

        ssize_t bytes_read = pread(fd_, buffer_, aligned_read, read_offset_);
        if (bytes_read < 0) {
            perror("pread failed");
            return traits_type::eof();
        }

        read_offset_ += to_read;
        setg(buffer_, buffer_, buffer_ + to_read);
        return traits_type::to_int_type(*gptr());
    }

private:
    int fd_;
    size_t file_size_;
    size_t read_offset_;
    size_t chunk_size_;
    char* buffer_;
};

class DirectIOStream : public Aws::IOStream {
public:
    DirectIOStream(const std::string& path, size_t chunk_size = 2 * 1024 * 1024)
        : Aws::IOStream(nullptr), buf_(path, chunk_size) {
        this->rdbuf(&buf_);
    }

    size_t GetFileSize() const {
        return buf_.GetFileSize();
    }

private:
    DirectIOStreamBuf buf_;
};

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


std::shared_ptr<Aws::IOStream> GetInputStream(const std::string& file_path, bool use_directio) {
    if (use_directio) {
        return std::make_shared<DirectIOStream>(file_path);
    } else {
        return std::make_shared<FileStream>(file_path, std::ios::binary);
#if 0
        return Aws::MakeShared<Aws::FStream>(
            "upload_tag", file_path.c_str(), std::ios_base::in | std::ios_base::binary
        );
#endif
    }
}

void UploadFile(std::shared_ptr<Aws::Transfer::TransferManager> transferManager,
                const std::string& file_path,
                const std::string& bucket,
                const std::string& s3_key,
                bool use_directio) {

#if 0
    auto streamFactory = [file_path]() -> std::shared_ptr<Aws::IOStream> {
        return std::make_shared<FileStream>(file_path); // or DirectIOStream
    };
    std::shared_ptr<Aws::IOStream> stream = GetInputStream(file_path, use_directio);
#endif

    std::cout << " File: " << file_path << " bucket: " << bucket <<" key: " << s3_key << " directIO: " << use_directio << std::endl;
#if 0
    if (use_directio) {
        stream = std::make_shared<DirectIOStream>(file_path);
    } else {
        stream = std::make_shared<FileStream>(file_path);
    }


    auto handle = transferManager->UploadFile(
        stream,
        bucket,
        s3_key,
        "application/octet-stream",
        Aws::Map<Aws::String, Aws::String>(),
        Aws::MakeShared<Aws::Client::AsyncCallerContext>("upload_ctx")
        );

        auto inputStream = Aws::MakeShared<Aws::FStream>(
            "upload_tag", file_path.c_str(), std::ios_base::in | std::ios_base::binary
        );
#endif

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
        std::cout << "Uploaded: " << s3_key << std::endl;
    } else {
        std::cerr << "Failed: " << s3_key
                  << " Error: " << handle->GetLastError().GetMessage() << std::endl;
    }
}

#if 0
std::shared_ptr<Aws::S3::S3Client> CreateS3Client() {
    Aws::Client::ClientConfiguration config;
    config.scheme = Aws::Http::Scheme::HTTPS;
    config.verifySSL = true; // Disable only if using self-signed certs
    config.caFile = "/usr/local/share/ca-certificates/minio.crt";
    config.requestTimeoutMs = 60000;   // 30 seconds or higher as needed
    config.connectTimeoutMs = 10000;   // connection timeout
    config.region = "us-east-1";
    config.enableClockSkewAdjustment = true;
    config.endpointOverride = "https://10.10.10.155:9000";
    config.maxConnections = 10;  // Match thread pool size

    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;
    return std::make_shared<Aws::S3::S3Client>(new Aws::S3::S3Client(Aws::Auth::AWSCredentials("minioadmin", "minioadmin"), config, signPayloads, false));

    //return std::shared_ptr<Aws::S3::S3Client>(new Aws::S3::S3Client(config,
    //			   Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false));
}
#endif

std::shared_ptr<Aws::S3::S3Client> CreateS3Client() {
    Aws::Client::ClientConfiguration config;
    config.scheme = Aws::Http::Scheme::HTTPS;
    config.verifySSL = true;
    config.region = "us-east-1";
    config.enableClockSkewAdjustment = true;
    config.endpointOverride = "10.10.10.155:9000";
    config.maxConnections = 64;

    // Add credentials explicitly (MinIO uses access/secret keys)
    Aws::Auth::AWSCredentials credentials("minioadmin", "minioadmin");

    // Create S3 client with credentials and path-style addressing
    auto s3_client = std::make_shared<Aws::S3::S3Client>(
        credentials,
        config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        true  // Enable virtual hosting = false (use path-style)
    );

    return s3_client;
}


std::shared_ptr<Aws::Transfer::TransferManager> CreateTransferManager(std::shared_ptr<Aws::S3::S3Client> s3Client) {

    unsigned int concurrency = std::thread::hardware_concurrency();
    if (concurrency == 0) concurrency = 4; // fallback

    auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("executor", 25);
    Aws::Transfer::TransferManagerConfiguration transferConfig(executor.get());
    transferConfig.s3Client = s3Client;
    transferConfig.bufferSize = 8 * 1024 * 1024; // 8MB

    return Aws::Transfer::TransferManager::Create(transferConfig);
}

#if 0
void UploadDirectory(const std::string& dir_path,
                     const std::string& bucket,
                     bool use_directio,
                     size_t thread_count = 4) {

    std::string endpointUrl = "https://10.10.10.155:9000";
    std::shared_ptr<Aws::S3::S3Client> s3Client = CreateS3Client(endpointUrl);
    std::shared_ptr<Aws::Transfer::TransferManager> transferManager = CreateTransferManager(s3Client);

    std::vector<std::future<void>> futures;

    DIR* dir = opendir(dir_path.c_str());
    if (!dir) {
        std::cerr << "Failed to open directory: " << dir_path << std::endl;
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type != DT_REG)
            continue;

        std::string file_name(entry->d_name);
        std::string full_path = dir_path + "/" + file_name;

        // Dispatch each file upload in a separate future
        futures.emplace_back(std::async(std::launch::async, [=]() {
            UploadFile(transferManager, full_path, bucket, file_name, use_directio);
        }));
    }

    closedir(dir);

    for (auto& f : futures) {
        f.get();
    }
}
void UploadDirectory(const std::string& dirPath,
                     const std::string& bucket,
                     bool useDirectIO,
                     size_t threadCount = 4) {
    auto s3Client = CreateS3Client("https://10.10.10.155:9000");
    auto transferManager = CreateTransferManager(s3Client);

    DIR* dir = opendir(dirPath.c_str());
    if (!dir) {
        std::cerr << "Failed to open directory: " << dirPath << std::endl;
        return;
    }

    std::vector<std::future<void>> tasks;
    for (dirent* entry; (entry = readdir(dir)) != nullptr;) {
        if (entry->d_type != DT_REG) continue;

        std::string fileName(entry->d_name);
        std::string fullPath = dirPath + "/" + fileName;

	std::cout << "full path: " << fullPath << std::endl;
        tasks.emplace_back(std::async(std::launch::async, [transferManager, fullPath, bucket, fileName, useDirectIO]() {
            UploadFile(transferManager, fullPath, bucket, fileName, useDirectIO);
        }));
    }
    closedir(dir);

    for (auto& task : tasks) task.get();
}
#endif


int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0]
                  << " <filename> <bucket> <key> <directio:0|1> \n";
        return 1;
    }

    Aws::SDKOptions options;
#if 0
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    auto consoleLogger = Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("ConsoleLogger", Aws::Utils::Logging::LogLevel::Trace);
    options.loggingOptions.logger_create_fn = [consoleLogger]() { return consoleLogger; };
#endif
    Aws::InitAPI(options);
    {
        std::string file_path = argv[1];
        std::string bucket = argv[2];
        std::string s3key = argv[3];
        bool use_directio = std::stoi(argv[4]) != 0;
        //size_t thread_count = std::stoul(argv[4]);
        Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true;  // Set to false only for testing with self-signed certs
        config.enableClockSkewAdjustment = true;
        config.maxConnections = 64;
        config.endpointOverride = "10.10.10.155:9000";  // MinIO endpoint
        config.region = "us-east-1";  // MinIO default region

        // Add credentials explicitly (MinIO uses access/secret keys)
        Aws::Auth::AWSCredentials credentials("minioadmin", "minioadmin");
        
        // Create S3 client with credentials and path-style addressing
        auto s3_client = std::make_shared<Aws::S3::S3Client>(
            credentials,
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            true  // Enable virtual hosting = false (use path-style)
        );

        // Configure TransferManager
        auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("ExecutorTag", 16);
        Aws::Transfer::TransferManagerConfiguration transferConfig(executor.get());
        transferConfig.s3Client = s3_client;
        transferConfig.bufferSize = 5 * 1024 * 1024;  // 5MB (MinIO minimum part size)

        auto transferManager = Aws::Transfer::TransferManager::Create(transferConfig);

        //UploadDirectory(dir_path, bucket, use_directio, thread_count);
	UploadFile(transferManager, file_path, bucket, s3key, use_directio);
    }
    Aws::ShutdownAPI(options);
    return 0;
}

