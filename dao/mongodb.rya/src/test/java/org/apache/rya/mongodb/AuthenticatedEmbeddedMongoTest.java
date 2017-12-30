package org.apache.rya.mongodb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.client.MongoIterable;

public class AuthenticatedEmbeddedMongoTest extends AuthenticatedMongoTestBase {
    private static final String USERNAME = "dbUser";
    private static final char[] PASSWORD = "dbPswd".toCharArray();

    @Override
    protected void updateMongoUsers() throws Exception {
        //addDBUser(USERNAME, PASSWORD, conf.getMongoDBName(), EmbeddedMongoFactory.DEFAULT_ADMIN_USER, EmbeddedMongoFactory.DEFAULT_ADMIN_PSWD);
    }

    @Test
    public void test() throws Exception {
        final List<String> expectedDatabases = Lists.newArrayList("rya", "test", "admin");
        final MongoIterable<String> rez = getAdminMongoClient().listDatabaseNames();
        final List<String> actual = new ArrayList<>();
        for(final String dbName : rez) {
            actual.add(dbName);
        }
        assertEquals(expectedDatabases, rez);
    }
}
