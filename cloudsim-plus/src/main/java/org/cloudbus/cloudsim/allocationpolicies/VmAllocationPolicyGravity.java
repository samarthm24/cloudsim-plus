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
	
	private final String failureZone;
	
	private Map<long,List<long>> savedAllocation;
	
	private final maxNeighborLookup;
	
	private final maxIterations;
    /**
     * Instantiates a VmAllocationPolicyRandom.
     *
     * @param random a Pseudo-Random Number Generator (PRNG) used to select a Host.
     *               The PRNG must return values between 0 and 1.
     */
	
	public VmAllocationPolicyGravity() {
		this("host",10,10);
	}
	
    public VmAllocationPolicyGravity(String failureZone,int maxNeighborLookup,int maxIterations){
        super();
        //this.random = Objects.requireNonNull(random);
        this.failureZone = failureZone;
        this.savedAllocation = new HashMap<>();
        this.maxIterations = maxIterations;
        this.maxNeighborLookup = maxNeighborLookup;
    }
    
    private long getCorrespondingHostId(Host host) {
    	if(this.failureZone == "host")
    		return host.getId();
    	if(this.failureZone == "rack")
    		return host.getRackId();
    	else
    		return host.getIsleId();
    }
    
    private List<Host> getHostListExcluding(List<long> idList){
    	List<Host> hostList = getHostList();
    	List<Integer> indeces = new ArrayList<Integer>();
    	for(int i=0;i<hostList.size();i++) {
    		Host host = hostList.get(i);
    		for(int j=0;j<idList.size();j++) {
    			long id = idList.get(i);
    			long hostId = getCorrespondingHostId(host);
    			if(hostId == id) {
    				indeces.add(i);
    				break;
    			}
    		}
    	}
    	for(int k=0;k<indeces.size();k++) {
    		int hostIndex = indeces.get(k);
    		hostList.remove(hostindex);
    	}
    	return hostList;
    } 

    @Override
    protected Optional<Host> defaultFindHostForVm(final Vm vm) {
        final List<Host> hostList = getHostList();
        /* The for loop just defines the maximum number of Hosts to try.
         * When a suitable Host is found, the method returns immediately. */
        final int maxTries = hostList.size();
        for (int i = 0; i < maxTries; i++) {
            final Host host = hostList.get(lastHostIndex);
            if (host.isSuitableForVm(vm)) {
                return Optional.of(host);
            }

            /* If it gets here, the previous Host doesn't have capacity to place the VM.
             * Then, moves to the next Host.
             * If the end of the Host list is reached, starts from the beginning,
             * until the max number of tries.*/
            lastHostIndex = ++lastHostIndex % hostList.size();
        }

        return Optional.empty();
    }
}
