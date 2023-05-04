//
// This file holds several functions specific to the workflow/rnaseq.nf in the nf-core/rnaseq pipeline
//

import okhttp3.*;
import okhttp3.HttpUrl;
import nextflow.Nextflow
import java.util.concurrent.CopyOnWriteArrayList
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel

class NwgcCore {

    private static Connection connection
    private static Channel channel
    private static String QUEUE_NAME = "nextflow";
    private static CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();
    private static registrationUrl = "";

    NwgcCore() {}

    public static void init(params) {
        // Setup the URL for OKHttp3
        registrationUrl = params.registration_url;
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(params.rabbitHost);
        factory.setUsername("nf");
        factory.setPassword("nf");
        
        try {
            connection = factory.newConnection()
            channel = connection.createChannel()
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        } catch (Exception e) {
            println e
        }
        
    }

    private static OkHttpClient getOkHttpClient() {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        return builder.build();
	}

    public static void dispatchRequest(String sampleId, String status) {
    	try {
    		OkHttpClient httpClient = getOkHttpClient();

            HttpUrl.Builder urlBuilder = HttpUrl.parse(registrationUrl).newBuilder();
            urlBuilder.addQueryParameter("sampleId", sampleId);
            urlBuilder.addQueryParameter("status", status);
            String url = urlBuilder.build().toString();

    		// Request - start building the request
			Request.Builder requestBuilder = new Request.Builder().url(url);
    		Request request = requestBuilder.build();
    		
    	    try (Response response = httpClient.newCall(request).execute()) {
				if (!response.isSuccessful())
					throw new Exception("Could Not Connect");
			}
    	}
    	catch (Exception e) {
            e.printStackTrace();
    		println "Error while attempting to send registration request to url";
    	}
	}

    public static void publishMessage(workflow, String message) {
        try {
            if (channel != null) {
                channel.basicPublish("", QUEUE_NAME, null, buildMessage(workflow, message).getBytes())
            }
        } catch (Exception e) {
            println e
        }
    }

    public static String buildMessage(workflow, String message) {
        return """
        {
            "message": "${message}"
            "launchDir": "${workflow.launchDir}"
        }
        """;
    }

    /***
     * This is called even if the process errors out
     */
    public static void processComplete(workflow, String sampleId) {
        if (workflow.success) {
            dispatchRequest(sampleId, "COMPLETE")
        }
        // if (connection != null && connection.isOpen() && workflow.success) {
        //     publishMessage(workflow, "process completed successfully")
        //     connection.close()
        // }
    }

    public static void error(workflow, sampleId) {
        dispatchRequest(sampleId, "ERROR")
        // publishMessage(workflow, sampleId, "error")
    }
}