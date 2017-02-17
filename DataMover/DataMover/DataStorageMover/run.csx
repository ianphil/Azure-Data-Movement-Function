using System.Net;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    log.Info($"C# HTTP trigger function processed a request. RequestUri={req.RequestUri}");

    // parse query parameter
    string name = req.GetQueryNameValuePairs()
        .FirstOrDefault(q => string.Compare(q.Key, "name", true) == 0)
        .Value;

    var sourceClient = GetSourceCloudBlobClient();
    var destinationClients = GetDestinationCloudBlobClients();

    //Parallel.ForEach(destinationClients, desitnationclient => {BlobCopy(name, log, sourceClient, destinationClient)});

    foreach(var destinationClient in destinationClients)
        BlobCopy(name, log, sourceClient, destinationClient);

    return name == null
        ? req.CreateResponse(HttpStatusCode.BadRequest, "Please pass a name on the query string or in the request body")
        : req.CreateResponse(HttpStatusCode.OK, "Copy started");
}

private static async Task BlobCopy(string sourceBlobName, TraceWriter log, CloudBlobClient sourceClient, CloudBlobClient destinationClient)
{
    string destinationBlobName = sourceBlobName;
    string sourceContainerName = "threat";
    string destinationContainerName = "threat";

    // Create the source CloudBlob instance
    CloudBlob sourceBlob = await GetCloudBlobAsync(sourceContainerName, sourceBlobName, BlobType.PageBlob, sourceClient);

    // Create the destination CloudBlob instance
    CloudBlob destinationBlob = await GetCloudBlobAsync(destinationContainerName, destinationBlobName, BlobType.PageBlob, destinationClient);

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
        log.Error(e.Message);
        log.Error("Transfer Error");
    }
}

public static async Task<CloudBlob> GetCloudBlobAsync(string containerName, string blobName, BlobType blobType, CloudBlobClient client)
{
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


private static CloudStorageAccount GetSourceStorageAccount()
{
    var connectionString = ConfigurationManager.AppSettings["source_STORAGE"];

    return CloudStorageAccount.Parse(connectionString);
}

private static List<CloudBlobClient> GetDestinationCloudBlobClients()
{
    var destinationkeys = new List<string>{ConfigurationManager.AppSettings["destination1_STORAGE"], ConfigurationManager.AppSettings["destination2_STORAGE"]};
    
    var DestinationCloudStorageAccounts = new List<CloudStorageAccount>();
    var DestinationCloudBlobClients = new List<CloudBlobClient>();

    foreach(var destination in destinationkeys)
    {
        var storageAccount = CloudStorageAccount.Parse(destination);
        var cloubBlobClient = storageAccount.CreateCloudBlobClient();

        DestinationCloudBlobClients.Add(cloubBlobClient);
    }

    return DestinationCloudBlobClients;
}
