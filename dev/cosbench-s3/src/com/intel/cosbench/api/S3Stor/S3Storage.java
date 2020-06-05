package com.intel.cosbench.api.S3Stor;

import static com.intel.cosbench.client.S3Stor.S3Constants.*;



import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.http.HttpStatus;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.lifecycle.LifecycleAndOperator;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilterPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleTagPredicate;
import com.intel.cosbench.api.storage.*;
import com.intel.cosbench.api.context.*;
import com.intel.cosbench.config.Config;
import com.intel.cosbench.driver.generator.Generators;
import com.intel.cosbench.driver.generator.NameGenerator;
import com.intel.cosbench.log.Logger;

public class S3Storage extends NoneStorage {
	private int timeout;
	
    private String accessKey;
    private String secretKey;
    private String endpoint;
    
    private AmazonS3 client;
    private List<ObjectTagMetadata> objTagMetadataList;
    private LifeCyleRulesMetadata lcMetadata;

    @Override
    public void init(Config config, Logger logger) {
    	super.init(config, logger);
    	
    	timeout = config.getInt(CONN_TIMEOUT_KEY, CONN_TIMEOUT_DEFAULT);
    	endpoint = config.get(ENDPOINT_KEY, ENDPOINT_DEFAULT);
        accessKey = config.get(AUTH_USERNAME_KEY, AUTH_USERNAME_DEFAULT);
        secretKey = config.get(AUTH_PASSWORD_KEY, AUTH_PASSWORD_DEFAULT);
        String objTagsDefn = config.get(OBJECT_TAGS_DEFN_KEY, "");
        String lcRulesCountDefn = config.get(LC_RULES_COUNT_KEY, "c(1)");
        String lcDefn = config.get(LC_RULES_DEFN_KEY, "");

        boolean pathStyleAccess = config.getBoolean(PATH_STYLE_ACCESS_KEY, PATH_STYLE_ACCESS_DEFAULT);
        
		String proxyHost = config.get(PROXY_HOST_KEY, "");
		String proxyPort = config.get(PROXY_PORT_KEY, "");
		
		parms.put(CONN_TIMEOUT_KEY, timeout);
        parms.put(ENDPOINT_KEY, endpoint);
    	parms.put(AUTH_USERNAME_KEY, accessKey);
    	parms.put(AUTH_PASSWORD_KEY, secretKey);
    	parms.put(PATH_STYLE_ACCESS_KEY, pathStyleAccess);
    	parms.put(PROXY_HOST_KEY, proxyHost);
    	parms.put(PROXY_PORT_KEY, proxyPort);
    	parms.put(OBJECT_TAGS_DEFN_KEY, objTagsDefn);
    	parms.put(LC_RULES_COUNT_KEY, lcRulesCountDefn);
    	parms.put(LC_RULES_DEFN_KEY, lcDefn);

//    	NameGenerator nmg = Generators.getNameGenerator("u(1,10)", "cbc", "bcb");
//    	Random tmpR = new Random();
//    	logger.debug("nmg next():{}", nmg.next(tmpR));
//
//        logger.debug("using storage config: {}", parms);
        
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setConnectionTimeout(timeout);
        clientConf.setSocketTimeout(timeout);
        clientConf.withUseExpectContinue(false);
        clientConf.withSignerOverride("S3SignerType");
        clientConf.setProtocol(Protocol.HTTP);
		if((!proxyHost.equals(""))&&(!proxyPort.equals(""))){
			clientConf.setProxyHost(proxyHost);
			clientConf.setProxyPort(Integer.parseInt(proxyPort));
		}
		
		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-west-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withClientConfiguration(clientConf)
                .withPathStyleAccessEnabled(pathStyleAccess);
//                .withChunkedEncodingDisabled(s3ClientProperties.isDisableChunkedEncoding())
//                .withPayloadSigningEnabled(s3ClientProperties.isPayloadSigningEnabled())
//                .withAccelerateModeEnabled(s3ClientProperties.isAccelerateModeEnabled())
//                .withDualstackEnabled(s3ClientProperties.isDualStackEnabled())
//                .withRequestHandlers(getRequestHandlers(s3ClientProperties))
//                .withMetricsCollector(getMetricsCollector(s3ClientProperties));

		client = builder.build();
        
        objTagMetadataList = setupObjectTags(objTagsDefn);
        lcMetadata = setupLifecycleRules(lcRulesCountDefn, lcDefn);
        
        logger.info("S3 client has been initialized with object tags metadata and lifecyle rules metadata");
    }
    
    /* This method parses the value provided to the 'object_tags' key parameter in the <storage> block */
    private List<ObjectTagMetadata> setupObjectTags(String objTagsDefn) {
    	List<ObjectTagMetadata> objTagMetadataList = null;
    	String[] defns = objTagsDefn.split(":");//   StringUtils.split(objTagsDefn, ":");
    	
    	logger.debug("Will be adding object tag metadata for definition: {}", objTagsDefn);
    	
    	if (defns != null) {
    		
    		objTagMetadataList = new ArrayList<ObjectTagMetadata>();
    		
	    	for(String defn : defns) {
	    		String[] parts = defn.split("\\|");
	    		logger.debug("setupObjectTags: defn: {}; parts: {}", defn, parts);
	    		if (parts != null ) {
	    			logger.debug("setupObjectTags: parts len: {}", parts.length);
	    		}
	    		
	    		if (parts != null && parts.length >= 3) {
	    			
	    			logger.debug("Will be adding object tag metadata for {}", parts);
	    			
	    			String valuePrefix = parts[1];
	    			NameGenerator nmg = Generators.getNameGenerator(parts[2], valuePrefix, "");
	    			
	    			ObjectTagMetadata otm = new ObjectTagMetadata(parts[0], nmg, new Random());
	    			objTagMetadataList.add(otm);
	    			
	    			logger.debug("Added object tag metadata for key '{}'", otm.getKey());
	    		}
	    	}
	    	
	    	logger.info("Done with setup of object tag metadata for {} tags", objTagMetadataList.size());
    	}
    	 
    	return objTagMetadataList;
    }
    
    private LifeCyleRulesMetadata setupLifecycleRules(String countDefn, String lcRules) {
    	
    	logger.debug("Setting up lifecycle rules for rule definition: {}", lcRules);
    	
    	String[] lcr = lcRules.split("=");
    	
    	if (lcr != null && lcr.length >= 2) {
    		NameGenerator countGenerator = Generators.getNameGenerator(countDefn, "", "");
    		
    		List<ObjectTagMetadata> otmList = setupObjectTags(lcr[0]);
    		
    		NameGenerator nmg = Generators.getNameGenerator(lcr[1], "", "");
    		
    		return new LifeCyleRulesMetadata(otmList, nmg, countGenerator, new Random());
    	}
    	
    	return null;
    }
    
    
    @Override
    public void setAuthContext(AuthContext info) {
        super.setAuthContext(info);
//        try {
//        	client = (AmazonS3)info.get(S3CLIENT_KEY);
//            logger.debug("s3client=" + client);
//        } catch (Exception e) {
//            throw new StorageException(e);
//        }
    }

    @Override
    public void dispose() {
        super.dispose();
        client = null;
    }

	@Override
    public InputStream getObject(String container, String object, Config config) {
        super.getObject(container, object, config);
        InputStream stream;
        try {
        	
            S3Object s3Obj = client.getObject(container, object);
            stream = s3Obj.getObjectContent();
            
        } catch (Exception e) {
            throw new StorageException(e);
        }
        return stream;
    }

    @Override
    public void createContainer(String container, Config config) {
        super.createContainer(container, config);
        try {
        	if(!client.doesBucketExist(container)) {
	            client.createBucket(container);
        	}
        	
        	applyBucketLifecycleConfiguration(container);
        	
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private void applyBucketLifecycleConfiguration(String container) {
    	List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<BucketLifecycleConfiguration.Rule>();
    	
    	if (lcMetadata != null) {
	    		String countStr = lcMetadata.getRulesCountGenerator().next(lcMetadata.getRandom());
	    		int count = Integer.valueOf(countStr);
	    		
	    		logger.debug("Will be creating {} lifecycle rules for container {}", count, container);
	    		
	    		for (int i = 0; i < count; ++i) {
	    		
	    		Rule r = new BucketLifecycleConfiguration.Rule();
	    		
	    		List<LifecycleFilterPredicate> lctpList = new ArrayList<LifecycleFilterPredicate>();
		    	for (ObjectTagMetadata otm : lcMetadata.objTagMetadataList) {
		    		Tag t = new Tag(otm.getKey(), otm.getGenerator().next(otm.getRandom()));
		    		
		    		logger.debug("applyBucketLifecycleConfiguration: Adding tag for key:{}, value:{}", t.getKey(), t.getValue());
		    		LifecycleTagPredicate lctp = new LifecycleTagPredicate(t);
		    		lctpList.add(lctp);
		    	}
		    	
		    	LifecycleFilter lcf = new LifecycleFilter(new LifecycleAndOperator(lctpList));
		    	int expiryDays = Integer.valueOf(lcMetadata.getExpiryDaysGenerator().next(lcMetadata.getRandom()));
		    	
		    	r = r.withFilter(lcf).withExpirationInDays(expiryDays).withStatus(BucketLifecycleConfiguration.ENABLED);
		    	rules.add(r);
		    	
		    	logger.info("Applied lifecycle configuration {} with expiry {} to bucket {}", new Object[] {lcf.toString(), expiryDays, container});
    		}
	    	
	    	BucketLifecycleConfiguration bucketLifecycleConfiguration = new BucketLifecycleConfiguration(rules);
	    	client.setBucketLifecycleConfiguration(container, bucketLifecycleConfiguration);
	    	
	    	logger.info("Applying bucket lifecycle config: {} to container: {}", bucketLifecycleConfiguration, container);
    	}
    }

	@Override
    public void createObject(String container, String object, InputStream data,
            long length, Config config) {
        super.createObject(container, object, data, length, config);
        try {
    		ObjectMetadata metadata = new ObjectMetadata();
    		metadata.setContentLength(length);
    		metadata.setContentType("application/octet-stream");
    		
    		PutObjectRequest por = new PutObjectRequest(container, object, data, metadata);
        	
        	// add object tags now if provided
        	if (objTagMetadataList != null) {
	        	List<Tag> tsList = new ArrayList<Tag>(2);
	        	
	        	for (ObjectTagMetadata tagMetadata : objTagMetadataList) {
	        		String tagVal = tagMetadata.getGenerator().next(tagMetadata.getRandom());
	        		Tag t = new Tag(tagMetadata.getKey(), tagVal);
	        		tsList.add(t);
	        	}
	        	
	        	if (!tsList.isEmpty()) {
	        		por = por.withTagging(new ObjectTagging(tsList));
	        	}
	        	
	        	logger.debug("Added tags to object:container:tags {}", new Object[] {object, container, tsList.size()});
        	}
        	
        	client.putObject(por);
        	
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteContainer(String container, Config config) {
        super.deleteContainer(container, config);
        try {
        	if(client.doesBucketExist(container)) {
        		client.deleteBucket(container);
        	}
        } catch(AmazonS3Exception awse) {
        	if(awse.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
        		throw new StorageException(awse);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void deleteObject(String container, String object, Config config) {
        super.deleteObject(container, object, config);
        try {
            client.deleteObject(container, object);
        } catch(AmazonS3Exception awse) {
        	if(awse.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
        		throw new StorageException(awse);
        	}
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    
    /* ObjectTagsMetadata - This class holds entities required to generate object tag values 
     * in a dynamic manner */
    class ObjectTagMetadata {
    	private NameGenerator nmg;
    	private Random rand;
    	private String key;
    

	    public ObjectTagMetadata(String k, NameGenerator n, Random r) {
	    	this.key = k;
	    	this.nmg = n;
	    	this.rand = r;
	    }
	    
	    public String getKey() {
	    	return key;
	    }
	    
	    public NameGenerator getGenerator() {
	    	return nmg;
	    }
	    
	    public Random getRandom() {
	    	return rand;
	    }
    };
    
    class LifeCyleRulesMetadata {
    	private List<ObjectTagMetadata> objTagMetadataList;
    	private NameGenerator expiryDaysGenerator;
    	private NameGenerator rulesCountGenerator;
    	private Random rand;
    	
    	public LifeCyleRulesMetadata(List<ObjectTagMetadata> otm, NameGenerator n1, NameGenerator n2, Random r) {
    		this.expiryDaysGenerator = n1;
    		this.rulesCountGenerator = n2;
    		this.rand = r;
    		this.objTagMetadataList = otm;
    	}
    	
    	public List<ObjectTagMetadata> getObjTagMetadataList() {
    		return objTagMetadataList;
    	}
    	
    	public NameGenerator getExpiryDaysGenerator() {
    		return expiryDaysGenerator;
    	}
    	
    	public NameGenerator getRulesCountGenerator() {
    		return rulesCountGenerator;
    	}
    	
    	public Random getRandom() {
    		return rand;
    	}
    }
}
