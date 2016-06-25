package mvm.rya.shell.command.accumulo.administrative;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Date;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import com.google.common.base.Optional;

import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import mvm.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import mvm.rya.api.instance.RyaDetails.GeoIndexDetails;
import mvm.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import mvm.rya.api.instance.RyaDetails.ProspectorDetails;
import mvm.rya.api.instance.RyaDetails.TemporalIndexDetails;
import mvm.rya.shell.AccumuloITBase;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.administrative.GetInstanceDetails;
import mvm.rya.shell.command.administrative.Install;
import mvm.rya.shell.command.administrative.Install.DuplicateInstanceNameException;
import mvm.rya.shell.command.administrative.Install.InstallConfiguration;

/**
 * Tests the methods of {@link AccumuloGetInstanceDetails}.
 */
public class AccumuloGetInstanceDetailsIT extends AccumuloITBase {

    @Test
    public void getDetails() throws AccumuloException, AccumuloSecurityException, DuplicateInstanceNameException, CommandException {
        // Install an instance of Rya.
        final String instanceName = "instance_name";
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableEntityCentricIndex(true)
                .setEnableFreeTextIndex(true)
                .setEnableTemporalIndex(true)
                .setEnablePcjIndex(true)
                .setEnableGeoIndex(true)
                .setFluoPcjAppName("fluo_app_name")
                .build();

        final Install install = new AccumuloInstall(getConnectionDetails(), getConnector(), new Authorizations());
        install.install(instanceName, installConfig);

        // Verify the correct details were persisted.
        final GetInstanceDetails getInstanceDetails = new AccumuloGetInstanceDetails(getConnectionDetails(), getConnector(), new Authorizations());
        final java.util.Optional<RyaDetails> details = getInstanceDetails.getDetails(instanceName);

        final RyaDetails expectedDetails = RyaDetails.builder()
                .setRyaInstanceName(instanceName)

                // The version depends on how the test is packaged, so just grab whatever was stored.
                .setRyaVersion( details.get().getRyaVersion() )

                .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails(new TemporalIndexDetails(true) )
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(true)
                            .setFluoDetails( new FluoDetails("fluo_app_name") )
                            .build())
                .setProspectorDetails( new ProspectorDetails(Optional.<Date>absent()) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.<Date>absent()) )
                .build();

        assertEquals(expectedDetails, details.get());
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void getDetails_instanceDoesNotExist() throws AccumuloException, AccumuloSecurityException, InstanceDoesNotExistException, CommandException {
        final GetInstanceDetails getInstanceDetails = new AccumuloGetInstanceDetails(getConnectionDetails(), getConnector(), new Authorizations());
        getInstanceDetails.getDetails("instance_name");
    }

    @Test
    public void getDetails_instanceDoesNotHaveDetails() throws AccumuloException, AccumuloSecurityException, InstanceDoesNotExistException, CommandException, TableExistsException {
        // Mimic a pre-details rya install.
        final String instanceName = "instance_name";

        final TableOperations tableOps = getConnector().tableOperations();

        final String spoTableName = instanceName + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
        final String ospTableName = instanceName + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX;
        final String poTableName = instanceName + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX;
        tableOps.create(spoTableName);
        tableOps.create(ospTableName);
        tableOps.create(poTableName);

        // Verify that the operation returns empty.
        final GetInstanceDetails getInstanceDetails = new AccumuloGetInstanceDetails(getConnectionDetails(), getConnector(), new Authorizations());
        final java.util.Optional<RyaDetails> details = getInstanceDetails.getDetails(instanceName);
        assertFalse( details.isPresent() );
    }
}