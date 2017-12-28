package org.apache.rya.mongodb;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

public class AuthenticatedEmbeddedMongoTest extends AuthenticatedMongoTestBase {
    private static final String ADMIN_USERNAME = "admin";
    private static final char[] ADMIN_PASSWORD = "password".toCharArray();

    private static final String USERNAME = "dbUser";
    private static final char[] PASSWORD = "dbPswd".toCharArray();
    @Override
    protected void updateMongoUsers() throws Exception {
        addAdminUser(ADMIN_USERNAME, ADMIN_PASSWORD);
    }

    @Override
    protected void createConfigAndClient(final MongoDBRdfConfiguration conf) throws Exception {
        //super.createAndSetStatefulConfig(conf, USERNAME, PASSWORD);
        super.createAndSetStatefulConfig(conf, ADMIN_USERNAME, ADMIN_PASSWORD);
    }

    @Test
    public void test() throws Exception {
        addDBUser(USERNAME, PASSWORD, conf.getMongoDBName(), ADMIN_USERNAME, ADMIN_PASSWORD);
        final List<String> expectedDatabases = Lists.newArrayList("rya", "test", "admin");
        final List<String> rez = getMongoClient().getDatabaseNames();
        assertEquals(expectedDatabases, rez);
    }
}
