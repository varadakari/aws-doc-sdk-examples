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
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    // Create a ConsoleLogSystem with desired log level
    auto consoleLogger = Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("ConsoleLogger", Aws::Utils::Logging::LogLevel::Trace);

    // Set the logger in SDK options
    options.loggingOptions.logger_create_fn = [consoleLogger]() { return consoleLogger; };

    Aws::InitAPI(options);
    {
    	Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true;
        config.enableClockSkewAdjustment = true;
        config.maxConnections = 64;
				     

        //Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;

        //auto s3_client = std::make_shared<Aws::S3::S3Client>(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()), config, signPayloads, false);
        //auto s3_client = std::make_shared<Aws::S3::S3Client>(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()), nullptr, config);
        auto s3_client = std::make_shared<Aws::S3::S3Client>(config);

        // Increase thread pool size to 50 for higher parallelism
        auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("ExecutorTag", 10);

        Aws::Transfer::TransferManagerConfiguration transferConfig(executor.get());
        transferConfig.s3Client = s3_client;
	    transferConfig.bufferSize = 16 * 1024 * 1024;  // 5MB

        auto transferManager = Aws::Transfer::TransferManager::Create(transferConfig);
	    Aws::String s3Key = prefix + "/test";

#if 0
        //auto handle = transferManager->UploadFile(filePath,bucket, s3Key,  "text/plain");
        auto handle = transferManager->UploadFile(
            filePath.c_str(),
            bucket.c_str(),
            s3Key.c_str(),
            "application/octet-stream",
            Aws::Map<Aws::String, Aws::String>());
        // Create the input stream
        auto inputStream = Aws::MakeShared<Aws::FStream>("UploadTag",
                                filePath.c_str(),
                                std::ios_base::in | std::ios_base::binary);
        std::shared_ptr<Aws::IOStream> inputStream = std::make_shared<std::fstream>(filePath, std::ios::in | std::ios::binary);


        // Check if stream opened successfully
        if (!inputStream->good()) {
            std::cerr << "Failed to open file " << filePath << std::endl;
            return 1;
        }

        // Upload using TransferManager
        auto handle = transferManager->UploadFile(
            inputStream,
            bucket,
            s3Key,
            "application/octet-stream",
            Aws::Map<Aws::String, Aws::String>(), // Metadata
            nullptr,
            filePath // Tag
        );
#endif
        auto inputStream = Aws::MakeShared<Aws::FStream>(
            "upload_tag", filePath.c_str(), std::ios_base::in | std::ios_base::binary
        );

        auto handle = transferManager->UploadFile(
            inputStream,
            bucket,
            s3Key,
            "application/octet-stream",
            Aws::Map<Aws::String, Aws::String>(),
            Aws::MakeShared<Aws::Client::AsyncCallerContext>("upload_ctx"),
            filePath
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

