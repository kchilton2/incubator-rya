package mvm.rya.shell.command.accumulo.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Iterator;

import org.apache.accumulo.core.client.Connector;
import org.calrissian.mango.collect.CloseableIterable;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;

public class LoadStatementTestUtils {

	public static void assertOnQuery(AccumuloRyaDAO dao, RyaQuery query, int numResults) throws RyaDAOException{
		CloseableIterable<RyaStatement> iter = dao.getQueryEngine().query(query);
		Iterator<RyaStatement> actIter = iter.iterator();
		int count = 0;
		while(actIter.hasNext()){
			assertNotNull(actIter.next());
			count++;
		}
		assertEquals(count, numResults);

	}
	
	public static AccumuloRyaDAO getDAO(AccumuloConnectionDetails connectionDetails, Connector conn, String instanceName) throws RyaDAOException{
		AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.set(ConfigUtils.CLOUDBASE_USER, connectionDetails.getUsername());
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, new String(connectionDetails.getPassword()));
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, connectionDetails.getInstanceName());
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, connectionDetails.getZookeepers());
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "U");
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, instanceName);
		AccumuloRyaDAO dao = new AccumuloRyaDAO();
		dao.setConf(conf);
		dao.setConnector(conn);
		dao.init();
		
		return dao;
	}

}
