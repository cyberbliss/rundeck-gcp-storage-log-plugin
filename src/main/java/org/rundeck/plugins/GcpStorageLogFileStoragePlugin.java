package org.rundeck.plugins;

import com.dtolabs.rundeck.core.dispatcher.DataContextUtils;
import com.dtolabs.rundeck.core.logging.ExecutionFileStorageException;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.plugins.ServiceNameConstants;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.descriptions.PluginProperty;
import com.dtolabs.rundeck.plugins.logging.ExecutionFileStoragePlugin;

import com.google.cloud.storage.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.MessageFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Plugin(service = ServiceNameConstants.ExecutionFileStorage, name = "org.rundeck.gcp-storage")
@PluginDescription(title = "GCP Storage", description = "Stores log files in a GCP Storage bucket")
public class GcpStorageLogFileStoragePlugin implements ExecutionFileStoragePlugin {

    public static final String DEFAULT_PATH_FORMAT = "project/${job.project}/${job.execid}";

    @PluginProperty(title = "Bucket name", required = true, description = "Bucket to store files in")
    private String bucket;

    @PluginProperty(
            title = "Path",
            required = true,
            description = "The path in the bucket to store a log file. " +
                    " Default: "
                    + DEFAULT_PATH_FORMAT +
                    "\n\nYou can use these expansion variables: \n\n" +
                    "* `${job.execid}` = execution ID\n" +
                    "* `${job.project}` = project name\n" +
                    "* `${job.id}` = job UUID (or blank).\n" +
                    "* `${job.group}` = job group (or blank).\n" +
                    "* `${job.name}` = job name (or blank).\n" +
                    "",
            defaultValue = DEFAULT_PATH_FORMAT)
    private String path;

    protected static Logger logger = Logger.getLogger(GcpStorageLogFileStoragePlugin.class.getName());
    protected Storage storageClient;
    protected String expandedPath;
    protected Map<String, ?> context;

    public GcpStorageLogFileStoragePlugin() {
        super();
        storageClient = StorageOptions.getDefaultInstance().getService();
    }

    // Used for testing
    public GcpStorageLogFileStoragePlugin(Storage storageClient) {
        super();
        this.storageClient = storageClient;
    }

    @Override
    public void initialize(Map<String, ?> context) {
        this.context = context;

        if(null == getBucket() || "".equals(getBucket().trim())) {
            throw new IllegalArgumentException("bucket was not set");
        }

        if(null == getPath() || "".equals(getPath().trim())) {
            throw new IllegalArgumentException("path was not set");
        }

        if(!getPath().contains("${job.execid}") && !getPath().endsWith("/")) {
            throw new IllegalArgumentException("path must contain ${job.execid} or end with /");
        }

        String configPath = getPath();
        if(!configPath.contains("${job.execid")) {
            configPath = configPath + "${job.execid}";
        }
        expandedPath = expandPath(configPath, context);

        if (null == expandedPath || "".equals(expandedPath.trim())) {
            throw new IllegalArgumentException("expanded value of path was empty");
        }
        if (expandedPath.endsWith("/")) {
            throw new IllegalArgumentException("expanded value of path must not end with /");
        }

        logger.debug("Path that will be used: " + expandedPath);
    }

    @Override
    public boolean isAvailable(String filetype) throws ExecutionFileStorageException {
        String filePath = resolvedFilepath(expandedPath, filetype);
        BlobId blobId = BlobId.of(bucket, filePath);
        try {
            Blob blob = storageClient.get(blobId);
            if (blob != null && blob.exists()) {
                return true;
            }
        }catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }

        return false;
    }

    @Override
    public boolean store(String filetype, InputStream stream, long length, Date lastModified) throws IOException, ExecutionFileStorageException {
        String filePath = resolvedFilepath(expandedPath, filetype);
        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, filePath).build();
        byte[] contents = IOUtils.toByteArray(stream);

        logger.debug(MessageFormat.format("Storing content to GCP bucket {0} path {1}", bucket, filePath));
        try {
            Blob blob = storageClient.create(
                    blobInfo,
                    contents
            );
        }catch(StorageException e) {
            logger.error(e.getMessage(), e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }
        return true;
    }

    @Override
    public boolean retrieve(String filetype, OutputStream stream) throws IOException, ExecutionFileStorageException {
        String filePath = resolvedFilepath(expandedPath, filetype);
        BlobId blobId = BlobId.of(bucket, filePath);
        try {
            Blob blob = storageClient.get(blobId);
            if (blob != null && blob.exists()) {
                byte[] content = blob.getContent();
                stream.write(content);
                stream.close();
            }
        }catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ExecutionFileStorageException(e.getMessage(), e);
        }
        return true;
    }

    protected String resolvedFilepath(final String path, final String filetype) {
        return path + "." + filetype;
    }

    /**
     * Expands the path format using the context data
     *
     * @param pathFormat format
     * @param context context data
     *
     * @return expanded path
     */
    static String expandPath(String pathFormat, Map<String, ?> context) {
        String result = pathFormat.replaceAll("^/+", "");
        if (null != context) {
            result = DataContextUtils.replaceDataReferencesInString(
                    result,
                    DataContextUtils.addContext("job", stringMap(context), new HashMap<>()),
                    null,
                    false,
                    true
            );

        }
        result = result.replaceAll("/+", "/");

        return result;
    }

    private static Map<String, String> stringMap(final Map<String, ?> context) {
        HashMap<String, String> result = new HashMap<>();
        for (String s : context.keySet()) {
            Object o = context.get(s);
            if (o != null) {
                result.put(s, o.toString());
            }
        }
        return result;
    }

    public static void main(String[] args) throws IOException, ExecutionFileStorageException {
        BasicConfigurator.configure();
        GcpStorageLogFileStoragePlugin lfPlugin = new GcpStorageLogFileStoragePlugin();
        String action = args[0];
        HashMap<String, Object> stringHashMap = new HashMap<>();
        stringHashMap.put("execid", "testexecid");
        stringHashMap.put("project", "testproject");

        lfPlugin.setPath(DEFAULT_PATH_FORMAT);
        lfPlugin.setBucket("rundeck-test-log-storage");

        lfPlugin.initialize(stringHashMap);

        String fileType = "rdlog";

        if ("store".equals(action)) {
            File file = new File(args[1]);
            lfPlugin.store(fileType, new FileInputStream(file), file.length(), new Date(file.lastModified()));
        } else if ("state".equals(action)) {
            logger.info("available? "+lfPlugin.isAvailable(fileType));
        } else if ("retrieve".equals(action)) {
            lfPlugin.retrieve(fileType, new FileOutputStream(new File(args[1])));
        }

    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
