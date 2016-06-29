package mvm.rya.shell.command.accumulo.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Iterator;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.calrissian.mango.collect.CloseableIterable;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetailsRepository;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;

public class LoadStatementTest {

	@Test
	public void test() throws Exception {
		Instance instance = new MockInstance("test");
		Connector conn = instance.getConnector("root", "");
		AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails("root", "".toCharArray(), "", "", "");
		RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(conn, "test_rya");
		detailsRepo.initialize(RyaDetails.builder().setRyaInstanceName("test_rya").setRyaVersion("3.2.10").
				build());
		RyaDetails ryaDetails = detailsRepo.getRyaInstanceDetails();
		AccumuloLoadStatement command = new AccumuloLoadStatement(connectionDetails, conn, new Authorizations());
		ValueFactory vf = ValueFactoryImpl.getInstance();
		
		Statement statement = vf.createStatement(vf.createURI("urn:tempSubj"), vf.createURI("urn:tempPred"), vf.createURI("urn:tempObj"));
		Statement statement2 = vf.createStatement(vf.createURI("urn:tempSubj"), vf.createURI("urn:tempPred2"), vf.createURI("urn:tempObj"));
		command.loadStatement("test_rya", statement, "");
		command.loadStatement("test_rya", statement2, "");
		
		// try to query the spo table
		AccumuloRyaDAO dao = LoadStatementTestUtils.getDAO(connectionDetails, conn, "test_rya");
		RyaStatement query = new RyaStatement();
		query.setSubject(new RyaURI("urn:tempSubj"));
		LoadStatementTestUtils.assertOnQuery(dao, new RyaQuery(query), 2);
	}
}
