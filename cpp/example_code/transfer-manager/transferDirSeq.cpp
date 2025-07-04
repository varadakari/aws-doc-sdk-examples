#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferManager.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <fstream>

bool IsRegularFile(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

void UploadDirectory(const std::shared_ptr<Aws::Transfer::TransferManager>& transferManager,
                     const std::string& directoryPath,
                     const std::string& bucketName,
                     const std::string& s3Prefix) {

    DIR* dir = opendir(directoryPath.c_str());
    if (!dir) {
        std::cerr << "Failed to open directory: " << directoryPath << "\n";
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string filename(entry->d_name);
        if (filename == "." || filename == "..") continue;

        std::string localPath = directoryPath + "/" + filename;
        if (!IsRegularFile(localPath)) continue;

        std::string s3Key = s3Prefix + "/" + filename;

        std::cout << "Uploading: " << localPath << " -> s3://" << bucketName << "/" << s3Key << "\n";

        auto handle = transferManager->UploadFile(
            localPath.c_str(),
            bucketName.c_str(),
            s3Key.c_str(),
            "application/octet-stream",
            Aws::Map<Aws::String, Aws::String>());

        handle->WaitUntilFinished();
        if (handle->GetStatus() == Aws::Transfer::TransferStatus::COMPLETED) {
            std::cout << "Upload succeeded: " << filename << "\n";
        } else {
            std::cerr << "Upload failed: " << filename
	    	      << " error: " << handle->GetStatus() << "\n";
        }
    }

    closedir(dir);
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <directory_path> <bucket_name> <s3_prefix>\n";
        return 1;
    }

    const std::string dirPath = argv[1];
    const std::string bucket = argv[2];
    const std::string prefix = argv[3];

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

        UploadDirectory(transferManager, dirPath, bucket, prefix);
    }
    Aws::ShutdownAPI(options);
    return 0;
}

