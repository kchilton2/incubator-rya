package mvm.rya.shell.command.accumulo.data;

import java.io.File;
import java.nio.file.Path;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetailsRepository;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;

public class LoadStatementFileTest {

	@Test
	public void test() throws Exception {
		Instance instance = new MockInstance("test");
		Connector conn = instance.getConnector("root", "");
		AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails("root", "".toCharArray(), "", "", "");
		RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(conn, "test_rya");
		detailsRepo.initialize(RyaDetails.builder().setRyaInstanceName("test_rya").setRyaVersion("3.2.10").
				build());
		RyaDetails ryaDetails = detailsRepo.getRyaInstanceDetails();
		AccumuloLoadStatementsFile command = new AccumuloLoadStatementsFile(connectionDetails, conn, new Authorizations());
		
		File file = new File("src/test/resources/sample.nt");
		Path path = file.toPath();
		command.load("test_rya", path);
		
		AccumuloRyaDAO dao = LoadStatementTestUtils.getDAO(connectionDetails, conn, "test_rya");
		RyaStatement query = new RyaStatement();
		query.setSubject(new RyaURI("http://www.w3.org/2001/sw/RDFCore/ntriples/"));
		LoadStatementTestUtils.assertOnQuery(dao, new RyaQuery(query), 3);
		
		 query = new RyaStatement();
		 query.setPredicate(new RyaURI("http://purl.org/dc/elements/1.1/publisher"));
		 LoadStatementTestUtils.assertOnQuery(dao, new RyaQuery(query), 1);

	}
}
