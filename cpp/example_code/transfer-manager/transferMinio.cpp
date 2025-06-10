#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h> // For Aws::FStream
#include <aws/core/utils/memory/stl/AWSAllocator.h> // For Aws::Allocator
#include <fstream>

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <file_path> <bucket_name> <s3_prefix>\n";
        return 1;
    }

    const Aws::String filePath = argv[1];
    const Aws::String bucket = argv[2];
    const Aws::String prefix = argv[3];
    Aws::SDKOptions options;
#if 0
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    auto consoleLogger = Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("ConsoleLogger", Aws::Utils::Logging::LogLevel::Trace);
    options.loggingOptions.logger_create_fn = [consoleLogger]() { return consoleLogger; };
#endif

    Aws::InitAPI(options);
    {
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
        Aws::String s3Key = prefix;

        auto inputStream = Aws::MakeShared<Aws::FStream>(
            "upload_tag", filePath.c_str(), std::ios_base::in | std::ios_base::binary
        );

        auto handle = transferManager->UploadFile(
            inputStream,
            bucket,
            s3Key,
            "application/octet-stream",
            Aws::Map<Aws::String, Aws::String>(),
            Aws::MakeShared<Aws::Client::AsyncCallerContext>("upload_ctx")
        );

        handle->WaitUntilFinished();

        if (handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED)
            std::cout << "Upload succeeded\n";
        else
            std::cerr << "Upload failed\n";
    }
    Aws::ShutdownAPI(options);
    return 0;
}

