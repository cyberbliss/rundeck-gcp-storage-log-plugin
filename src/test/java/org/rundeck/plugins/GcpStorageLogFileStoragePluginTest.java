package org.rundeck.plugins;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class GcpStorageLogFileStoragePluginTest {

    final String DEFAULT_FILETYPE = "rdlog";

    @Test
    public void expandPathLeadingSlashIsRemoved() {
        Assert.assertEquals("foo", GcpStorageLogFileStoragePlugin.expandPath("/foo", testContext()));
    }

    @Test
    public void expandPathMultiSlashRemoved() {
        Assert.assertEquals("foo/bar/test", GcpStorageLogFileStoragePlugin.expandPath("/foo//bar///test",
                testContext()));
    }

    @Test
    public void expandExecId() {
        Assert.assertEquals("foo/testexecid/bar", GcpStorageLogFileStoragePlugin.expandPath("foo/${job.execid}/bar",
                testContext()));
    }

    @Test
    public void expandProject() {
        Assert.assertEquals("foo/testproject/bar", GcpStorageLogFileStoragePlugin.expandPath("foo/${job.project}/bar",
                testContext()));
    }

    @Test
    public void missingKey() {
        Assert.assertEquals("foo/bar", GcpStorageLogFileStoragePlugin.expandPath("foo/${job.id}/bar", testContext()));
    }

    @Test
    public void expandJobId() {
        Assert.assertEquals("foo/testjobid/bar", GcpStorageLogFileStoragePlugin.expandPath("foo/${job.id}/bar",
                testContext2()));
    }

    @Test
    public void initializeNoBucket() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        try {
            testPlugin.initialize(testContext());
            Assert.fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("bucket was not set"));
        }
    }

    @Test
    public void initializeNoPath() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        try {
            testPlugin.setBucket("testbucket");
            testPlugin.initialize(testContext());
            Assert.fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path was not set"));
        }
    }

    @Test
    public void initializeInvalidPath() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        try {
            testPlugin.setBucket("testbucket");
            testPlugin.setPath("foo/bar");
            testPlugin.initialize(testContext());
            Assert.fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("path must contain ${job.execid} or end with /"));
        }
    }

    @Test
    public void initializeInvalidEmptyPath() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        try {
            testPlugin.setBucket("testbucket");
            testPlugin.setPath("${job.execid}");
            testPlugin.initialize(testContext3());
            Assert.fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("expanded value of path was empty"));
        }
    }

    @Test
    public void initializeInvalidSlashPath() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        try {
            testPlugin.setBucket("testbucket");
            testPlugin.setPath("${job.execid}/");
            testPlugin.initialize(testContext3());
            Assert.fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("expanded value of path must not end with /"));
        }
    }

    @Test
    public void initialize() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        try {
            testPlugin.setBucket("testbucket");
            testPlugin.setPath(GcpStorageLogFileStoragePlugin.DEFAULT_PATH_FORMAT);
            testPlugin.initialize(testContext());
            Assert.assertEquals("project/testproject/testexecid", testPlugin.expandedPath);
        } catch (Exception e) {
            Assert.fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    @Test
    public void store() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        boolean result = false;
        InputStream testStream = new ByteArrayInputStream("mytest".getBytes());
        try {
            testPlugin.setBucket("testbucket");
            testPlugin.setPath(GcpStorageLogFileStoragePlugin.DEFAULT_PATH_FORMAT);
            testPlugin.initialize(testContext());

            result = testPlugin.store(DEFAULT_FILETYPE, testStream, 0, null);

        } catch (Exception e) {
            Assert.fail("Should not have thrown exception: " + e.getMessage());
        }

        Assert.assertTrue(result);
    }

    @Test
    public void isAvailable() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        store();
        boolean result = false;
        try {
            testPlugin.setBucket("testbucket");
            testPlugin.setPath(GcpStorageLogFileStoragePlugin.DEFAULT_PATH_FORMAT);
            testPlugin.initialize(testContext());

            result = testPlugin.isAvailable(DEFAULT_FILETYPE);

        } catch (Exception e) {
            Assert.fail("Should not have thrown exception: " + e);
        }
        Assert.assertTrue(result);
    }

    @Test
    public void retrieve() {
        Storage storage = LocalStorageHelper.getOptions().getService();
        GcpStorageLogFileStoragePlugin testPlugin = new GcpStorageLogFileStoragePlugin(storage);
        store();
        boolean result = false;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            testPlugin.setBucket("testbucket");
            testPlugin.setPath(GcpStorageLogFileStoragePlugin.DEFAULT_PATH_FORMAT);
            testPlugin.initialize(testContext());


            result = testPlugin.retrieve(DEFAULT_FILETYPE, stream);

            Assert.assertTrue(result);
            Assert.assertEquals("mytest", stream.toString("UTF-8"));
        } catch (Exception e) {
            Assert.fail("Should not have thrown exception: " + e);
        }

    }

    private HashMap<String, Object> testContext() {
        HashMap<String, Object> stringHashMap = new HashMap<String, Object>();
        stringHashMap.put("execid", "testexecid");
        stringHashMap.put("project", "testproject");
        stringHashMap.put("url", "http://rundeck:4440/execution/9/show");
        stringHashMap.put("serverUrl", "http://rundeck:4440");
        stringHashMap.put("serverUUID", "123");
        return stringHashMap;
    }

    private HashMap<String, ?> testContext2() {
        HashMap<String, Object> stringHashMap = testContext();
        stringHashMap.put("id", "testjobid");
        return stringHashMap;
    }

    private HashMap<String, ?> testContext3() {
        HashMap<String, Object> stringHashMap = testContext();
        stringHashMap.put("execid", "");
        return stringHashMap;
    }
}