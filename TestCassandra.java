import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.*;
import com.datastax.driver.core.utils.*;
import java.util.List;
import java.util.stream.Collectors;
import java.security.Security;
public class TestCassandra {
//10.239.204.120
       private static String CASSANDRA_IP="10.239.204.119";
//      private static String CASSANDRA_IP="10.239.204.120";
       private Cluster cluster;
       private Session session;
       public Session getSession() {
               return session;
       }
       public void connect(String node, Integer port) {
       //      String[] cipher =
       //      SSLOptions options = new SSLOptions(SSLContext.getDefault(), CIPHER);
               cluster = Cluster.builder().addContactPoint(node)
                               .withPort(port)
                               .withSSL()
                               .build();
               session = cluster.connect();
       }
       public void close() {
               session.close();
               cluster.close();
       }
       public void createKeyspace(String keyspaceName, String replicationStrategy, int replicationFactor) {
               StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                                       .append(keyspaceName).append(" WITH replication = {")
                                       .append("'class':'").append(replicationStrategy)
                                       .append("','replication_factor':").append(replicationFactor)
                                       .append("};");
               String query = sb.toString();
               session.execute(query);
       }
       public static void reportSecurityProviders() {
               for (int i=0; i<Security.getProviders().length; i++) {
                       System.out.println(Security.getProviders()[i]);
               }
       }
       public static void main(String[] args) {
               reportSecurityProviders();
               TestCassandra tc = new TestCassandra();
               tc.connect(CASSANDRA_IP, 9042);
//              KeyspaceRepository kr = new KeyspaceRepository(tc.getSession());
               String keyspace = "library";
//              kr.createKeyspace(keyspace, "SimpleStrategy", 1);
               System.out.println("session "+tc.getSession());
               ResultSet res = tc.getSession().execute("SELECT * FROM system_schema.keyspaces;");
               List<String> k = res.all()
                                       .stream()
                                       .filter(r -> r.getString(0).equals(keyspace.toLowerCase()))
                                       .map(r -> r.getString(0))
                                       .collect(Collectors.toList());
               System.out.println("matchedKeySpace" + k.size());
               System.out.println("matchedKeySpace name" + k.get(0));
       }
}
