package io.trino.filesystem.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.core.util.ClientOptions;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

import static io.trino.filesystem.azure.AzureUtils.blobCustomerProvidedKey;
import static io.trino.filesystem.azure.AzureUtils.lakeCustomerProvidedKey;

// simple java client trying to test onelake-azure sdk compatiblity
public class AzureOneLake
{

    private static final String CLIENT_ID = "fill in your sp creds";
    private static final String CLIENT_SECRET = " ";
    private static final String TENANT_ID = " ";

    private static final String ACCOUNT_URL = "https://onelake.dfs.fabric.microsoft.com/";
    private static final String FILE_SYSTEM_NAME = "SB/test_lh.Lakehouse/Tables/";  // Like a container or directory
    private static final String FILE_PATH = "publicholidays/_delta_log/00000000000000000000.json";

    public static ClientSecretCredential getClientSecretCredential()
    {
        return new ClientSecretCredentialBuilder()
                .clientId(CLIENT_ID)
                .clientSecret(CLIENT_SECRET)
                .tenantId(TENANT_ID)
                .build();
    }

    public static void main(String[] args)
            throws IOException
    {
        try {
            TokenCredential credential = getClientSecretCredential();

            /*BlockBlobClient builder = new BlobContainerClientBuilder()
                    .credential(credential)
                    .endpoint("https://onelake.dfs.fabric.microsoft.com")
                    .containerName("SB").buildClient()
                    .getBlobClient(FILE_SYSTEM_NAME)
                    .getBlockBlobClient();
            builder.exists();*/ // does not work

            // Create a DataLakeServiceClient
            DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClientBuilder()
                    .credential(credential)
                    .endpoint(ACCOUNT_URL)
                    .buildClient();

           //  dataLakeServiceClient.createFileSystem("SB").exists(); // works
           //  dataLakeServiceClient.createFileSystem("SB/test_lh.Lakehouse").exists(); // does not work

            // dataLakeServiceClient.getFileSystemClient("SB").getFileClient("test_lh.Lakehouse/").getProperties()  // does not work
            // dataLakeServiceClient.getFileSystemClient("SB/test_lh.Lakehouse/").getFileClient("Tables/").getProperties().isDirectory() // works

            dataLakeServiceClient.getFileSystemClient(FILE_SYSTEM_NAME)
                    .listPaths(new ListPathsOptions().setRecursive(true), null).forEach(path -> {
                System.out.println("Path: " + path.getName());
            });

            // Get a file client for the target file
            DataLakeFileClient fileClient = dataLakeServiceClient
                    .getFileSystemClient(FILE_SYSTEM_NAME)
                    .getFileClient(FILE_PATH);


            // Read the file contents
           ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            fileClient.read(outputStream);

            // Print the file content
            System.out.println("File content: " + outputStream);
            System.out.println("Read operation completed.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
