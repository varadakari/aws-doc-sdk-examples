#include <aws/core/Aws.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>
#include <aws/transfer/TransferHandle.h>

#include <iostream>
#include <thread>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

using namespace Aws::Transfer;
using namespace Aws::S3;

void WaitForTransfers(const std::shared_ptr<TransferManager>& transferManager) {
    transferManager->WaitUntilAllFinished();
    std::cout << "\nAll transfers completed.\n";
}

bool file_exists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

std::shared_ptr<TransferManager> CreateTransferManagerWithCallbacks() {
    Aws::Client::ClientConfiguration config;
    config.scheme = Aws::Http::Scheme::HTTPS;
    config.verifySSL = true;  // Set to false only for testing with self-signed certs
    config.enableClockSkewAdjustment = true;
    config.maxConnections = 64;
    config.endpointOverride = "10.10.10.155:9000";  // MinIO endpoint
    config.region = "us-east-1";  // MinIO default region
    const std::string ca_cert_path = "/usr/local/share/ca-certificates/minio.crt";
    if (file_exists(ca_cert_path)) {
        config.caFile = ca_cert_path;
    }

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
    transferConfig.transferBufferMaxHeapSize = 512 * 1024 * 1024;
    transferConfig.bufferSize = 8 * 1024 * 1024;                  // 8 MB per buffer

    // === Upload and Download Progress Callback ===
    transferConfig.uploadProgressCallback = [](const TransferManager*,
                                                const std::shared_ptr<const TransferHandle>& handle) {
        std::cout << "[UPLOAD PROGRESS] " << handle->GetKey()
                  << ": " << handle->GetBytesTransferred() << " / " << handle->GetBytesTotalSize()
                  << " bytes\n";
    };

    transferConfig.downloadProgressCallback = [](const TransferManager*,
                                                  const std::shared_ptr<const TransferHandle>& handle) {
        std::cout << "[DOWNLOAD PROGRESS] " << handle->GetKey()
                  << ": " << handle->GetBytesTransferred() << " / " << handle->GetBytesTotalSize()
                  << " bytes\n";
    };

    // === Transfer Initiated Callback ===
    transferConfig.transferInitiatedCallback = [](const TransferManager*,
                                                   const std::shared_ptr<const TransferHandle>& handle) {
        std::cout << "[INITIATED] " << handle->GetKey() << "\n";
    };

    // === Transfer Status Updated Callback ===
    transferConfig.transferStatusUpdatedCallback = [](const TransferManager*,
                                                       const std::shared_ptr<const TransferHandle>& handle) {
        std::cout << "[STATUS] " << handle->GetKey()
                  << " status: " << handle->GetStatus() << "\n";
    };

    // === Error Callback ===
    transferConfig.errorCallback = [](const TransferManager*,
                                       const std::shared_ptr<const TransferHandle>& handle,
                                       const Aws::Client::AWSError<Aws::S3::S3Errors>& error) {
        std::cerr << "[ERROR] " << handle->GetKey()
                  << ": " << error.GetExceptionName() << " - " << error.GetMessage() << "\n";
    };

    auto tm = TransferManager::Create(transferConfig);

    return tm;
}

void UploadDirectory(const std::shared_ptr<TransferManager>& tm,
                     const Aws::String& localDir,
                     const Aws::String& bucket,
                     const Aws::String& s3Prefix) {
    std::cout << "Uploading directory: " << localDir << " → s3://" << bucket << "/" << s3Prefix << "\n";
    tm->UploadDirectory(localDir, bucket, s3Prefix, Aws::Map<Aws::String, Aws::String>());
    WaitForTransfers(tm);
}

void DownloadDirectory(const std::shared_ptr<TransferManager>& tm,
                       const Aws::String& bucket,
                       const Aws::String& s3Prefix,
                       const Aws::String& localDir) {
    std::cout << "Downloading directory: s3://" << bucket << "/" << s3Prefix << " → " << localDir << "\n";
    tm->DownloadToDirectory(localDir, bucket, s3Prefix);
    WaitForTransfers(tm);
}

int main(int argc, char* argv[]) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        if (argc < 3) {
            std::cerr << "Usage:\n"
                      << "  cloudcp <local-dir> s3://bucket/prefix\n"
                      << "  cloudcp s3://bucket/prefix <local-dir>\n";
            return 1;
        }

        std::string first = argv[1];
        std::string second = argv[2];

        // Create TransferManager with callbacks and config-based region
        //auto transferManager = CreateTransferManagerWithCallbacks();
        Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true;  // Set to false only for testing with self-signed certs
        config.enableClockSkewAdjustment = true;
        config.maxConnections = 64;
        config.endpointOverride = "10.10.10.155:9000";  // MinIO endpoint
        config.region = "us-east-1";  // MinIO default region
        const std::string ca_cert_path = "/usr/local/share/ca-certificates/minio.crt";
        if (file_exists(ca_cert_path)) {
            config.caFile = ca_cert_path;
        }

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
        auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("ExecutorTag", 32);
        Aws::Transfer::TransferManagerConfiguration transferConfig(executor.get());
        transferConfig.s3Client = s3_client;
        transferConfig.transferBufferMaxHeapSize = 512 * 1024 * 1024;
        transferConfig.bufferSize = 8 * 1024 * 1024;                  // 8 MB per buffer

        // === Transfer Initiated Callback ===
        transferConfig.transferInitiatedCallback = [](const TransferManager*,
                                                       const std::shared_ptr<const TransferHandle>& handle) {
            std::cout << "[INITIATED] " << handle->GetKey() << "\n";
        };

        // === Error Callback ===
        transferConfig.errorCallback = [](const TransferManager*,
                                           const std::shared_ptr<const TransferHandle>& handle,
                                           const Aws::Client::AWSError<Aws::S3::S3Errors>& error) {
            std::cerr << "[ERROR] " << handle->GetKey()
                      << ": " << error.GetExceptionName() << " - " << error.GetMessage() << "\n";
        };

        auto transferManager = TransferManager::Create(transferConfig);

        if (first.rfind("s3://", 0) == 0) {
            // Download mode
            Aws::String s3Uri = first;
            Aws::String localDir = second;

            Aws::String bucket, prefix;
            Aws::String path = s3Uri.substr(5);  // remove "s3://"
            auto slashPos = path.find('/');
            if (slashPos == Aws::String::npos) {
                std::cerr << "Invalid S3 URI format: " << s3Uri << "\n";
                return 1;
            }
            bucket = path.substr(0, slashPos);
            prefix = path.substr(slashPos + 1);

            std::cout << "dir: " << localDir << " bucket: " << bucket <<" prefix: " << prefix << std::endl;
            DownloadDirectory(transferManager, bucket, prefix, localDir);

        } else if (second.rfind("s3://", 0) == 0) {
            // Upload mode
            Aws::String localDir = first;
            Aws::String s3Uri = second;

            Aws::String bucket, prefix;
            Aws::String path = s3Uri.substr(5);  // remove "s3://"
            auto slashPos = path.find('/');
            if (slashPos == std::string::npos) {
                std::cerr << "Invalid S3 URI format: " << s3Uri << "\n";
                return 1;
            }
            bucket = path.substr(0, slashPos);
            prefix = path.substr(slashPos + 1);

            std::cout << "dir: " << localDir << " bucket: " << bucket <<" prefix: " << prefix << std::endl;
            UploadDirectory(transferManager, localDir, bucket, prefix);

        } else {
            std::cerr << "❌ Unable to determine upload or download direction.\n";
            return 1;
        }
    }
    Aws::ShutdownAPI(options);
    return 0;
}

