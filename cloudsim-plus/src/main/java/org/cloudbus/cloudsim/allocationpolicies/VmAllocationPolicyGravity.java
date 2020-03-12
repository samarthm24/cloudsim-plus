package org.cloudbus.cloudsim.allocationpolicies;

import java.io.*; 
import java.lang.*; 
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationMedianAbsoluteDeviation;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationStaticThreshold;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.hosts.HostStateHistoryEntry;
import org.cloudbus.cloudsim.power.models.PowerModelLinear;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicyMinimumUtilization;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelPlanetLab;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import java.util.Random;



public class VmAllocationPolicyGravity extends VmAllocationPolicyAbstract implements VmAllocationPolicy {
    /**
     * A Pseudo-Random Number Generator (PRNG) used to select a Host.
     */
//    private final ContinuousDistribution random;
    
    private DatacenterBroker broker;

    /**
     * Instantiates a VmAllocationPolicyRandom.
     *
     * @param random a Pseudo-Random Number Generator (PRNG) used to select a Host.
     *               The PRNG must return values between 0 and 1.
     */
    public VmAllocationPolicyGravity(DatacenterBroker broker){
        super();
        this.broker = broker;
        //this.random = Objects.requireNonNull(random);
    }

    @Override
    protected Optional<Host> defaultFindHostForVm(final Vm vm) {
    	final List<Host> hostList = getHostList();
        /* The for loop just defines the maximum number of Hosts to try.
         * When a suitable Host is found, the method returns immediately. */
        final int maxTries = hostList.size();
        for (int i = 0; i < maxTries; i++) {
            final Host host = hostList.get(lastHostIndex);
            //Different from the FirstFit policy, it always increments the host index.
            lastHostIndex = ++lastHostIndex % hostList.size();
            if (host.isSuitableForVm(vm)) {
                return Optional.of(host);
            }
        }

        return Optional.empty();
    }
}
