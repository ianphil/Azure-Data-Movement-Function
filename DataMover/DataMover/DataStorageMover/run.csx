using System.Net;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using System.Threading;
using System.Threading.Tasks;

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    log.Info($"C# HTTP trigger function processed a request. RequestUri={req.RequestUri}");

    // parse query parameter
    string name = req.GetQueryNameValuePairs()
        .FirstOrDefault(q => string.Compare(q.Key, "name", true) == 0)
        .Value;

    if (name != null)
    {
        await BlobCopy(name, log);
    }

    return name == null
        ? req.CreateResponse(HttpStatusCode.BadRequest, "Please pass a name on the query string or in the request body")
        : req.CreateResponse(HttpStatusCode.OK, "Copy started");
}

private static async Task BlobCopy(string sourceBlobName, TraceWriter log)
{
    string destinationBlobName = sourceBlobName;
    string sourceContainerName = "source";
    string destinationContainerName = "destination";

    // Create the source CloudBlob instance
    CloudBlob sourceBlob = await GetCloudBlobAsync("source", sourceContainerName, sourceBlobName, BlobType.PageBlob);

    // Create the destination CloudBlob instance
    CloudBlob destinationBlob = await GetCloudBlobAsync("destination", destinationContainerName, destinationBlobName, BlobType.PageBlob);

    // Create CancellationTokenSource used to cancel the transfer
    CancellationTokenSource cancellationSource = new CancellationTokenSource();

    TransferCheckpoint checkpoint = null;
    SingleTransferContext context = new SingleTransferContext();

    // Start the transfer
    try
    {
        await TransferManager.CopyAsync(sourceBlob, destinationBlob, false /* isServiceCopy */, null /* options */, context, cancellationSource.Token);
    }
    catch (Exception e)
    {
        log.Error("Transfer Error");
    }
}

public static async Task<CloudBlob> GetCloudBlobAsync(string location, string containerName, string blobName, BlobType blobType)
{
    CloudBlobClient client;

    switch (location)
    {
        case "source":
            client = GetSourceCloudBlobClient();
            break;
        case "destination":
            client = GetDestinationCloudBlobClient();
            break;
        default:
            throw new ArgumentException(string.Format("Invalid client type {0}", location), "clientType");
    }

    CloudBlobContainer container = client.GetContainerReference(containerName);
    await container.CreateIfNotExistsAsync();

    CloudBlob cloudBlob;
    switch (blobType)
    {
        case BlobType.AppendBlob:
            cloudBlob = container.GetAppendBlobReference(blobName);
            break;
        case BlobType.BlockBlob:
            cloudBlob = container.GetBlockBlobReference(blobName);
            break;
        case BlobType.PageBlob:
            cloudBlob = container.GetPageBlobReference(blobName);
            break;
        case BlobType.Unspecified:
        default:
            throw new ArgumentException(string.Format("Invalid blob type {0}", blobType.ToString()), "blobType");
    }

    return cloudBlob;
}

private static CloudBlobClient GetSourceCloudBlobClient()
{
    return GetSourceStorageAccount().CreateCloudBlobClient();
}

private static CloudBlobClient GetDestinationCloudBlobClient()
{
    return GetDestinationStorageAccount().CreateCloudBlobClient();
}

private static CloudStorageAccount GetSourceStorageAccount()
{
    var key = "<key>";
    var connectionString = $"DefaultEndpointsProtocol=https;AccountName=<storage_account>;AccountKey={key}";

    return CloudStorageAccount.Parse(connectionString);
}

private static CloudStorageAccount GetDestinationStorageAccount()
{
    var key = "<key>";
    var connectionString = $"DefaultEndpointsProtocol=https;AccountName=<stroage_account>;AccountKey={key}";

    return CloudStorageAccount.Parse(connectionString);
}