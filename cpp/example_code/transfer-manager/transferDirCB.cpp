#include <aws/core/Aws.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/transfer/TransferManager.h>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <dirent.h>    // For directory traversal on POSIX systems
#include <sys/stat.h>  // For file info

// Thread-safe console output helper
std::mutex coutMutex;
void SafePrint(const std::string& msg) {
    std::lock_guard<std::mutex> lock(coutMutex);
    std::cout << msg << std::endl;
}

// List files in a directory (non-recursive)
std::vector<Aws::String> ListFilesInDirectory(const Aws::String& directory) {
    std::vector<Aws::String> files;

    DIR* dir = opendir(directory.c_str());
    if (!dir) {
        SafePrint("Failed to open directory: " + directory);
        return files;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name(entry->d_name);
        if (name == "." || name == "..") continue;

        std::string fullPath = directory + "/" + name;

        struct stat s;
        if (stat(fullPath.c_str(), &s) == 0 && S_ISREG(s.st_mode)) {
            files.push_back(fullPath.c_str());
        }
    }

    closedir(dir);
    return files;
}

// Convert local file path to S3 key by stripping base directory prefix
Aws::String MakeS3Key(const Aws::String& baseDir, const Aws::String& filePath) {
    if (filePath.find(baseDir) == 0) {
        Aws::String key = filePath.substr(baseDir.length());
        if (!key.empty() && (key[0] == '/' || key[0] == '\\')) {
            key.erase(0, 1);
        }
        return key;
    }
    return filePath;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <bucket-name> <local-directory>" << std::endl;
        return 1;
    }

    const Aws::String bucketName = argv[1];
    const Aws::String localDirectory = argv[2];

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        // Setup S3 client configuration
        Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = false;
        config.endpointOverride = "https://10.10.10.155:9000";
        config.region = "us-east-1";
        const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;

        auto s3Client = std::make_shared<Aws::S3::S3Client>(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()), config, signPayloads, false);

        //auto s3Client = Aws::MakeShared<Aws::S3::S3Client>("S3Client", clientConfig);

        // Create TransferManager with thread pool executor
        auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("Executor", 16);
    	Aws::Transfer::TransferManagerConfiguration transferConfig(executor.get());
        transferConfig.s3Client = s3Client;
        transferConfig.bufferSize = 4 * 1024 * 1024;



	    transferConfig.transferStatusUpdatedCallback =
            [](const Aws::Transfer::TransferManager*, const std::shared_ptr<const Aws::Transfer::TransferHandle>& handle) {
            auto status = handle->GetStatus();
            if (status == Aws::Transfer::TransferStatus::COMPLETED) {
                SafePrint("Upload succeeded: ");
            } else if (status == Aws::Transfer::TransferStatus::FAILED) {
                SafePrint("Upload failed: Error: " + handle->GetLastError().GetMessage());
            } else if (status == Aws::Transfer::TransferStatus::CANCELED) {
                SafePrint("Upload canceled: ");
            }
        };

#if 0
        transferConfig.uploadProgressCallback =
            [](const Aws::Transfer::TransferManager*, const std::shared_ptr<const Aws::Transfer::TransferHandle>& handle) {
                  SafePrint("Upload progress: " +
                  std::to_string(handle->GetBytesTransferred()) + "/" +
                  std::to_string(handle->GetBytesTotalSize()));
        };
#endif



        auto transferManager = Aws::Transfer::TransferManager::Create(transferConfig);

        // List files in the directory (non-recursive)
        std::vector<Aws::String> files = ListFilesInDirectory(localDirectory);
        if (files.empty()) {
            SafePrint("No files found in directory: " + localDirectory);
            Aws::ShutdownAPI(options);
            return 0;
        }

        SafePrint("Found " + std::to_string(files.size()) + " files to upload.");

        for (const auto& filePath : files) {
            Aws::String s3Key = MakeS3Key(localDirectory, filePath);

            auto handle = transferManager->UploadFile(
                filePath,
                bucketName,
                s3Key,
                "application/octet-stream",
                Aws::Map<Aws::String, Aws::String>{} // No extra metadata
            );
        }

	// Wait for all uploads to complete
            transferManager->WaitUntilAllFinished();

        SafePrint("All uploads completed.");
    }
    Aws::ShutdownAPI(options);
    return 0;
}

