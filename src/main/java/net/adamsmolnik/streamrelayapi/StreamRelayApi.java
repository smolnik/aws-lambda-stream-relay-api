package net.adamsmolnik.streamrelayapi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Base64;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author asmolnik
 *
 */
public class StreamRelayApi implements RequestStreamHandler {

	private static final String S3_BUCKET = getProperty("s3Bucket");

	private static final String S3_KEY = getProperty("s3Key");

	private static final String REGION = getProperty("region");

	private static final AmazonS3 S3 = AmazonS3ClientBuilder.standard().withRegion(REGION).build();

	@Override
	public void handleRequest(InputStream is, OutputStream os, Context context) throws IOException {
		ObjectMapper om = new ObjectMapper();
		DispatchRequest dispatchRequest = om.readValue(is, DispatchRequest.class);
		context.getLogger().log("dispatchKey: " + dispatchRequest.dispatchKey);
		try (InputStream s3is = S3.getObject(S3_BUCKET, S3_KEY).getObjectContent();
				OutputStream os64 = Base64.getEncoder().wrap(os)) {
			byte[] buf = new byte[8192];
			int bytesRead;
			while ((bytesRead = s3is.read(buf)) != -1) {
				os64.write(buf, 0, bytesRead);
			}
		}
	}

	private static String getProperty(String key) {
		String value = System.getProperty(key);
		return value == null ? System.getenv(key) : value;
	}

}
