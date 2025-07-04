#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <fstream>
#include <iostream>

int main(int argc, char **argv) {
    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    // Create a ConsoleLogSystem with desired log level
    auto consoleLogger = Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("ConsoleLogger", Aws::Utils::Logging::LogLevel::Trace);

    // Set the logger in SDK options
    options.loggingOptions.logger_create_fn = [consoleLogger]() { return consoleLogger; };
    Aws::InitAPI(options);
    {
        if (argc < 5) {
            std::cerr << argv[0] << " <bucket> <key> <file> <endpoint|none>\n";
            return 1;
        }

        Aws::String bucket_name = argv[1];
        Aws::String object_key = argv[2];
        Aws::String file_name = argv[3];
        Aws::String endpoint_url = argv[4];

        Aws::S3Crt::S3CrtClientConfiguration clientConfig;
	clientConfig.region = "us-west-1";
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.verifySSL = true; // use with caution
        clientConfig.useVirtualAddressing = false; // for non-AWS S3
	clientConfig.caFile = "/usr/local/share/ca-certificates/minio.crt";
	clientConfig.enableTcpKeepAlive = true;
        clientConfig.tcpKeepAliveIntervalMs = 30000;
        //clientConfig.connectTimeoutMs = 3000;
        //clientConfig.requestTimeoutMs = 5000;

        if (endpoint_url != "none") {
            std::cout << "Setting up endpoint url " << endpoint_url << std::endl;
            clientConfig.endpointOverride = endpoint_url;
        }

	Aws::Auth::AWSCredentials creds("minioadmin", "minioadmin"); // or your MinIO access key & secret
								     //
								     //
        //if AWS
	//auto credentialsProvider = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("S3Upload");
        //Aws::S3Crt::S3CrtClient s3crt_client(credentialsProvider, clientConfig);
	//else
	Aws::S3Crt::S3CrtClient s3crt_client(creds, clientConfig);

        Aws::S3Crt::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_key);

        auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", file_name.c_str(),
                            std::ios_base::in | std::ios_base::binary);

        if (!input_data->good()) {
            std::cerr << "Failed to open file " << file_name << std::endl;
            return 1;
        }

        request.SetBody(input_data);

        std::cout << "Uploading object...\n";
        auto outcome = s3crt_client.PutObject(request);

        if (outcome.IsSuccess()) {
            std::cout << "Successfully uploaded " << object_key << " to " << bucket_name << std::endl;
        } else {
            std::cerr << "Upload failed: " << outcome.GetError().GetMessage() << std::endl;
        }
    }
    Aws::ShutdownAPI(options);
    Aws::Utils::Logging::ShutdownAWSLogging();
    return 0;
}

