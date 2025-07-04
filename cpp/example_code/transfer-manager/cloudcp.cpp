#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/FileSystemUtils.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/transfer/TransferManager.h>
#include <aws/transfer/TransferHandle.h>
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
#include <regex>
#include <dirent.h>

bool parseS3Uri(const std::string& uri, std::string& bucket, std::string& prefix, std::string& objectKey) {
    const std::string prefix_str = "s3://";
    if (uri.substr(0, prefix_str.length()) != prefix_str) return false;

    std::string rest = uri.substr(prefix_str.length());
    size_t firstSlash = rest.find('/');

    if (firstSlash == std::string::npos) {
        // No object path given
        bucket = rest;
        prefix.clear();
        objectKey.clear();
        return true;
    }

    bucket = rest.substr(0, firstSlash);
    std::string keyPath = rest.substr(firstSlash + 1);  // everything after bucket/

    size_t lastSlash = keyPath.find_last_of('/');
    if (lastSlash == std::string::npos) {
        // No prefix, only object
        prefix.clear();
        objectKey = keyPath;
    } else {
        prefix = keyPath.substr(0, lastSlash);
        objectKey = keyPath.substr(lastSlash + 1);
    }

    return true;
}

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

int DownloadFile(std::shared_ptr<Aws::Transfer::TransferManager> transferManager,
                 const std::string& bucket,
                 const Aws::String& key,
                 const std::string& full_path) {
    make_parent_dirs(full_path);

    auto handle = transferManager->DownloadFile(bucket, key, full_path);

    handle->WaitUntilFinished();

    if (handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED) {
        //std::cout << "Download succeeded: " << full_path << std::endl;
        //std::cout << "Etag: " << handle->GetEtag() << std::endl;
    } else {
        std::cerr << "Download failed. Status: "
                  << static_cast<int>(handle->GetStatus()) << std::endl;
        return 1;
    }

    return 0;
}

bool file_exists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

int main(int argc, char** argv) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        std::string endpointUrl;
        std::vector<std::string> positionalArgs;

        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];

            if (arg == "--endpoint-url") {
                if (i + 1 < argc) {
                    endpointUrl = argv[++i];
                } else {
                    std::cerr << "--endpoint-url requires a value\n";
                    Aws::ShutdownAPI(options);
                    return 1;
                }
            } else if (arg.rfind("-", 0) == 0) {
                // Unknown option, skip silently
                continue;
            } else {
                // Positional argument
                if (positionalArgs.size() < 2) {
                    positionalArgs.push_back(arg);
                } else {
                    // More than two positional args, ignore or handle error if needed
                    // For now, just ignore extra positional args
                }
            }
        }


        if (positionalArgs.size() != 2) {
            std::cerr << "Usage:\n"
                  << "  cloudcp [--endpoint-url <url>] /path/to/file s3://bucket[/prefix/]\n"
                  << "  cloudcp [--endpoint-url <url>] s3://bucket[/prefix/] /path/to/file\n";
            Aws::ShutdownAPI(options);
            return 1;
        }

        std::string src = positionalArgs[0];
        std::string dst = positionalArgs[1];
    	bool isMinIO = false;

        Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true; // Disable only if using self-signed certs
        config.requestTimeoutMs = 60000;   // 30 seconds or higher as needed
        config.connectTimeoutMs = 10000;   // connection timeout
        config.enableClockSkewAdjustment = true;
        config.maxConnections = 64;
        if (!endpointUrl.empty()) {
            config.endpointOverride = endpointUrl.c_str();
        }
        const std::string ca_cert_path = "/usr/local/share/ca-certificates/minio.crt";
        if (file_exists(ca_cert_path)) {
            isMinIO = true;
            config.caFile = ca_cert_path;
        }

        std::shared_ptr<Aws::S3::S3Client> s3_client;
        if (isMinIO) {
            s3_client = std::make_shared<Aws::S3::S3Client>(
                config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                true  // path-style access
            );
        } else {
            s3_client = std::make_shared<Aws::S3::S3Client>(config);  // default AWS behavior
        }


        // Configure TransferManager
        auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("ExecutorTag", 16);
        Aws::Transfer::TransferManagerConfiguration transferConfig(executor.get());
        transferConfig.s3Client = s3_client;
        transferConfig.bufferSize = 50 * 1024 * 1024;

        auto transferManager = Aws::Transfer::TransferManager::Create(transferConfig);

    	std::string bucket, prefix, objectKey;

        if (parseS3Uri(src, bucket, prefix, objectKey)) {
            // Download
            std::string localPath = dst;
            std::string key = prefix.empty() ? objectKey : (prefix + "/" + objectKey);
            if (DownloadFile(transferManager, bucket, key, localPath)) {
                Aws::ShutdownAPI(options);
                return 1;
            }
        } else if (parseS3Uri(dst, bucket, prefix, objectKey)) {
            // Upload
            std::string localPath = src;
            std::string filename = localPath.substr(localPath.find_last_of("/\\") + 1);
            std::string key = prefix.empty() ? filename : (prefix + "/" + filename);
            if (UploadFile(transferManager, localPath, bucket, key)) {
                Aws::ShutdownAPI(options);
                return 1;
            }
#if 0
            Aws::S3::Model::HeadObjectRequest headRequest;
            headRequest.SetBucket(bucket);
            headRequest.SetKey(key);

            auto headOutcome = s3_client->HeadObject(headRequest);
            if (headOutcome.IsSuccess()) {
                std::string etag = headOutcome.GetResult().GetETag();
                std::cout << "ETag: " << etag << std::endl;
            } else {
                std::cerr << "Failed to get ETag: " << headOutcome.GetError().GetMessage() << std::endl;
            }
#endif
        } else {
            std::cerr << "Invalid source or destination. One must be s3://bucket/key\n";
            Aws::ShutdownAPI(options);
            return 1;
        }
	transferManager.reset();
	s3_client.reset();
    }
    Aws::ShutdownAPI(options);
    return 0;
}
