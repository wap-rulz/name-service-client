package ds.adeesha.naming;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class NameServiceClient {
    private final EtcdClient etcdClient;

    public NameServiceClient(String nameServiceAddress) {
        etcdClient = new EtcdClient(nameServiceAddress);
    }

    public static String buildServerDetailsEntry(String serviceAddress, int port, String protocol) {
        return new JSONObject().put("ip", serviceAddress).put("port", Integer.toString(port)).put("protocol", protocol).toString();
    }

    public ServiceDetails findService(String serviceName) throws InterruptedException, IOException {
        System.out.println("Searching for details of service : " + serviceName);
        String etcdResponse = etcdClient.get(serviceName);
        ServiceDetails serviceDetails = new ServiceDetails().populate(etcdResponse);
        while (serviceDetails == null) {
            System.out.println("Couldn't find details of service: " + serviceName + ", retrying in 5 seconds.");
            Thread.sleep(5000);
            etcdResponse = etcdClient.get(serviceName);
            serviceDetails = new ServiceDetails().populate(etcdResponse);
        }
        return serviceDetails;
    }

    public void registerService(String serviceName, String IPAddress, int port, String protocol) throws IOException {
        String serviceInfoValue = buildServerDetailsEntry(IPAddress, port, protocol);
        etcdClient.put(serviceName, serviceInfoValue);
    }

    public class ServiceDetails {
        private String IPAddress;
        private int port;
        private String protocol;

        ServiceDetails populate(String serverResponse) {
            JSONObject serverResponseJSONObject = new JSONObject(serverResponse);
            if (serverResponseJSONObject.has("kvs")) {
                JSONArray values = serverResponseJSONObject.getJSONArray("kvs");
                JSONObject firstValue = (JSONObject) values.get(0);
                String encodedValue = (String) firstValue.get("value");
                byte[] serverDetailsBytes = Base64.getDecoder().decode(encodedValue.getBytes(StandardCharsets.UTF_8));
                JSONObject serverDetailsJson = new JSONObject(new String(serverDetailsBytes));
                IPAddress = serverDetailsJson.get("ip").toString();
                port = Integer.parseInt(serverDetailsJson.get("port").toString());
                protocol = serverDetailsJson.get("protocol").toString();
                return this;
            } else {
                return null;
            }
        }

        public String getIPAddress() {
            return IPAddress;
        }

        public int getPort() {
            return port;
        }

        public String getProtocol() {
            return protocol;
        }
    }
}
