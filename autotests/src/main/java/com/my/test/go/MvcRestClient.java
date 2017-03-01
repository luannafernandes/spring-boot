package com.my.test.go;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.Optional;

/**
 * Created by dumin on 3/1/17.
 */
public class MvcRestClient {

    public static void main(String... s) {
    }

    public Optional<User> getUserById(Integer userId) {
        Optional<User> user = Optional.empty();

        try {

            Client client = Client.create();

            WebResource webResource = client
                    .resource("http://localhost:8080/user/" + userId);

            ClientResponse response = webResource.accept("application/json")
                    .get(ClientResponse.class);

            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatus());
            }

            JSONObject output = get(response.getEntity(String.class));
            user = Optional.of(UserMapper.map(output));

        } catch (Exception e) {
            e.printStackTrace();
        }
        return user;

    }

    static JSONObject get(String response) {
        try {
            return new JSONObject(response);
        } catch (ParseException ex) {
            throw new RuntimeException("Error during rest call", ex);
        }
    }

}
