/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.redzoo.article.planetcassandra.reactive.service.completable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;

import net.oneandone.troilus.Dao;
import net.oneandone.troilus.DaoImpl;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;

import eu.redzoo.article.planetcassandra.reactive.CassandraDB;
import eu.redzoo.article.planetcassandra.reactive.WebContainer;
import eu.redzoo.article.planetcassandra.reactive.service.Hotel;
 

public class RunExample {
    
    public static void main(String[] args) throws InterruptedException, IOException {
        
        try (CassandraDB cassandra = CassandraDB.newInstance()) {
            try (WebContainer server = new WebContainer("/service")) {
    
                
                // START DB
                cassandra.executeCql("CREATE TABLE hotels (id text, " +
                                     "                     name text, " +
                                     "                     description text, " +
                                     "                     classification int, " +
                                     "                     picture_uri text, " +
                                     "                     room_ids set<text>, " +
                                     "                     PRIMARY KEY (id));");

                Session session = cassandra.getSession();
                Dao hotelsDao = new DaoImpl(session, "hotels");

                System.setProperty("cassandra_port", Integer.toString(cassandra.getPort()));
                System.setProperty("keyspace", cassandra.getKeyspace());
                
                // START WEBSERVER
                server.setPort(0);
                server.start();
                
                System.out.println(server.getBaseUrl());
                
                
                filldb(hotelsDao, server);
                
                        
                
                // perform the example
                perform(server);
            }
        }
    }
    
    

    
    
    public static void perform(WebContainer server) {
        Client client =  ResteasyClientBuilder.newClient();
        
        
        byte[] picture = client.target(server.getBaseUrl() + "/hotels/BUP932432/thumbnail")
                .request()
                .get(byte[].class);

        if (!Objects.equals((byte) -46, picture[342])) {
            throw new RuntimeException("resized hotel image thumbnail expected");
        }

        
        
        // hotel entry does not exits
        try {
            client.target(server.getBaseUrl() + "/hotels/BUPnotexits/thumbnail")
                  .request()
                  .get(byte[].class);
            
            throw new RuntimeException("NotFoundException expected");
        } catch (NotFoundException expected) { } 

        
        
        // hotel entry with broken URI
        picture = client.target(server.getBaseUrl() + "/hotels/BUP14334/thumbnail")
                        .request()
                        .get(byte[].class);

        if (!Objects.equals((byte) 98, picture[342])) {
            throw new RuntimeException("resized default hotel error image thumbnail expected");
        }
        
        
        System.out.println("tests completed");
    }
    
    
    
    
    
    

    
    private static void filldb(Dao hotelsDao, WebContainer server) {
        
        hotelsDao.writeEntity(new Hotel("BUP45544", 
                                        "Corinthia Budapest",
                                        ImmutableSet.of("1", "2", "3", "122", "123", "124", "322", "333"),
                                        null,
                                        Optional.of(5), 
                                        Optional.of("Superb hotel housed in a heritage building - exudes old world charm")
                                       ))
                 .execute();

  
        
        
        hotelsDao.writeWithKey("id", "BUP932432")
                 .value("name", "City Budapest")
                 .value("room_ids", ImmutableSet.of("1", "2", "3", "4", "5"))
                 .value("classification", 4)
                 .value("picture_uri", server.getBaseUrl()+ "/pictures/4545")
                 .execute();

   
    
        hotelsDao.writeEntity(new Hotel("BUP14334", 
                                        "Richter Panzio",
                                        ImmutableSet.of("1", "2", "3"),
                                        "http://localhost:" + server.getLocalPort() + "/doesnotexits",
                                        Optional.of(2), 
                                        Optional.empty())
                                        )
                  .withConsistency(ConsistencyLevel.ANY)
                  .execute();
    }    
}
