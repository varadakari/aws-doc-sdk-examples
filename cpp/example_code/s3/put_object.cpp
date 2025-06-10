#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <fstream>
#include <iostream>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>

using namespace Aws;
using namespace Aws::Auth;

int main(int argc, char **argv) {
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
	if( argc < 5 ) {
	    printf("%s <bucket name> <key> <file> <endpoint url>\n", argv[0]);
	    exit(1);
        }

        Aws::String bucket_name = argv[1];
        Aws::String object_key = argv[2];       // The name for the object in S3
        Aws::String file_name = argv[3];      // The path to your local file
        Aws::String endpoint_url = argv[4];  // Custom endpoint
        Aws::String region = "us-east-1";             // Region (used for signing)

        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = region;
        clientConfig.endpointOverride = endpoint_url;
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.verifySSL = true; // Disable only if using self-signed certs
        clientConfig.caFile = "/home/ubuntu/varada/minio.crt";
	const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";

        
	Aws::S3::S3Client s3_client(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()),
			 	    clientConfig,
                                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                    false);

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_key);

        auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", file_name.c_str(), std::ios_base::in | std::ios_base::binary);

        if (!input_data->good()) {
            std::cerr << "Failed to open file " << file_name << std::endl;
            return 1;
        }
        request.SetBody(input_data);

	printf("Setting the object \n");

        auto outcome = s3_client.PutObject(request);

        if (outcome.IsSuccess()) {
            std::cout << "Successfully uploaded " << object_key << " to " << bucket_name << std::endl;
        } else {
            std::cerr << "Upload failed: " << outcome.GetError().GetMessage() << std::endl;
        }
    }
    Aws::ShutdownAPI(options);
    return 0;
}

