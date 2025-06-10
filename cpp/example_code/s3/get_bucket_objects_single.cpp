#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <memory>

constexpr size_t BLOCK_SIZE = 4096;

bool make_parent_dirs(const std::string& path) {
    size_t pos = 0;
    while ((pos = path.find('/', pos + 1)) != std::string::npos) {
        std::string dir = path.substr(0, pos);
        if (!dir.empty()) {
            mkdir(dir.c_str(), 0755);
        }
    }
    return true;
}

void* aligned_alloc_block(size_t alignment, size_t size) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) return nullptr;
    return ptr;
}

bool download_object_direct_io(const Aws::S3::S3Client& client, const std::string& bucket, const Aws::String& key, const std::string& target_base) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket.c_str());
    request.SetKey(key);

    auto outcome = client.GetObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "Failed to download: " << key << " - " << outcome.GetError().GetMessage() << "\n";
        return false;
    }

    std::string full_path = target_base + "/" + key.c_str();
    make_parent_dirs(full_path);

    int fd = open(full_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if (fd < 0) {
        std::cerr << "Failed to open " << full_path << ": " << strerror(errno) << "\n";
        return false;
    }

    constexpr size_t BLOCK_SIZE = 4096;
    constexpr size_t CHUNK_SIZE = 2 * 1024 * 1024;  // 2MB
    size_t aligned_chunk_size = ((CHUNK_SIZE + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;

    std::unique_ptr<char, decltype(&free)> aligned_buf(
        static_cast<char*>(aligned_alloc_block(BLOCK_SIZE, aligned_chunk_size)), &free
    );

    if (!aligned_buf) {
        std::cerr << "Failed to allocate aligned 2MB buffer\n";
        close(fd);
        return false;
    }

    auto& stream = outcome.GetResult().GetBody();

    while (stream.good()) {
        stream.read(aligned_buf.get(), CHUNK_SIZE);
        std::streamsize bytes_read = stream.gcount();

        if (bytes_read <= 0) break;

        // Zero out padding
        size_t padded = ((bytes_read + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        if (padded > (size_t)bytes_read) {
            memset(aligned_buf.get() + bytes_read, 0, padded - bytes_read);
        }

        ssize_t written = write(fd, aligned_buf.get(), padded);
        if (written < 0) {
            std::cerr << "Write failed: " << strerror(errno) << "\n";
            close(fd);
            return false;
        }
    }

    close(fd);
    return true;
}


int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <bucket-name> <target-dir> [prefix]\n";
        return 1;
    }

    std::string bucket = argv[1];
    std::string target_dir = argv[2];
    std::string prefix = (argc > 3) ? argv[3] : "";

    Aws::SDKOptions options;
#if 0
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    // Create a ConsoleLogSystem with desired log level
    auto consoleLogger = Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("ConsoleLogger", Aws::Utils::Logging::LogLevel::Trace);

    // Set the logger in SDK options
    options.loggingOptions.logger_create_fn = [consoleLogger]() { return consoleLogger; };
#endif
    Aws::InitAPI(options);
    {
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-east-1";
        clientConfig.endpointOverride = "https://10.10.10.155:9000";
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.verifySSL = true; // Disable only if using self-signed certs
        clientConfig.caFile = "/home/bryck/varada/minio.crt";
	const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";

        
	Aws::S3::S3Client client(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()),
			 	    clientConfig,
                                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                    false);


        Aws::S3::Model::ListObjectsV2Request list_req;
        list_req.SetBucket(bucket.c_str());
        if (!prefix.empty()) list_req.SetPrefix(prefix.c_str());

        Aws::String continuation_token;

        do {
            if (!continuation_token.empty()) {
                list_req.SetContinuationToken(continuation_token);
            }

            auto list_outcome = client.ListObjectsV2(list_req);
            if (!list_outcome.IsSuccess()) {
                std::cerr << "ListObjects error: " << list_outcome.GetError().GetMessage() << "\n";
                break;
            }

            const auto& objects = list_outcome.GetResult().GetContents();
            for (const auto& obj : objects) {
                std::cout << "Downloading: " << obj.GetKey() << "\n";
                download_object_direct_io(client, bucket, obj.GetKey(), target_dir);
            }

            continuation_token = list_outcome.GetResult().GetNextContinuationToken();

        } while (!continuation_token.empty());
    }
    Aws::ShutdownAPI(options);
    return 0;
}

