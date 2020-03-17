package org.cloudbus.cloudsim.allocationpolicies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;

import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.vms.Vm;

import java.util.Random;


public class VmAllocationPolicyGravity extends VmAllocationPolicyAbstract implements VmAllocationPolicy {
    /**
     * A Pseudo-Random Number Generator (PRNG) used to select a Host.
     */
//    private final ContinuousDistribution random;
	
	private final String failureZone;
	
	private Map<Long,List<Long>> savedAllocation;
	
	private final long maxNeighborLookup;
	
	private final long maxIterations;
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
        this.savedAllocation = new HashMap<Long,List<Long>>();
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
    
    private List<Host> getHostListExcluding(List<Long> idList){
    	List<Host> hostListDatacenter = (List<Host>) getHostList();
    	List<Host> hostList = new ArrayList<Host>();
    	for(Host host: hostListDatacenter) {
    		hostList.add(host);
    	}
    	ArrayList<Integer> indeces = new ArrayList<Integer>();
    	for(int i=0;i<hostList.size();i++) {
    		Host host = hostList.get(i);
    		for(int j=0;j<idList.size();j++) {
    			long id = idList.get(j);
    			long hostId = getCorrespondingHostId(host);
    			if(hostId == id) {
    				indeces.add(i);
    				break;
    			}
    		}
    	}
    	for(int k=0;k<indeces.size();k++) {
    		int hostIndex = indeces.get(k);
    		hostList.remove(hostIndex);
    	}
    	return hostList;
    }
    
    private double getTotalPowerDatacenter(Host currentHost,Vm vm) {
    	List<Host> hostList = (List<Host>) getHostList();
    	double totalPower = 0.0;
    	for(int i=0;i<hostList.size();i++) {
    		Host host = hostList.get(i);
    		if(host == currentHost) {
    			totalPower += getPowerAfterAllocation(currentHost,vm);
     		}
    		else {
    			totalPower += host.getPowerModel().getPower(host.getCpuPercentUtilization());
    		}
    	}
    	return totalPower;
    }
    
    
	protected double getPowerAfterAllocation(final Host host, final Vm vm) {
	    try {
	        return host.getPowerModel().getPower(getMaxUtilizationAfterAllocation(host, vm));
	    } catch (IllegalArgumentException e) {
	//        LOGGER.error("Power consumption for {} could not be determined: {}", host, e.getMessage());
	    }
	
	    return 0;
	}

	protected double getMaxUtilizationAfterAllocation(final Host host, final Vm vm) {
	    final double requestedTotalMips = vm.getCurrentRequestedTotalMips();
	    final double hostUtilizationMips = host.getCpuMipsUtilization();
	    final double hostPotentialMipsUse = hostUtilizationMips + requestedTotalMips;
	    return hostPotentialMipsUse / host.getTotalMipsCapacity();
	}
	
	public static int randInt(int min, int max) {
	    // NOTE: This will (intentionally) not run as written so that folks
	    // copy-pasting have to think about how to initialize their
	    // Random instance.  Initialization of the Random instance is outside
	    // the main scope of the question, but some decent options are to have
	    // a field that is initialized once and then re-used as needed or to
	    // use ThreadLocalRandom (if using at least Java 1.7).
	    // 
	    // In particular, do NOT do 'Random rand = new Random()' here or you
	    // will get not very good / not very random results.
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}



    @Override
    protected Optional<Host> defaultFindHostForVm(final Vm vm) {
        List<Host> hostList = (List<Host>)getHostList();
        Host currentHost,minHost = hostList.get(0);
        long vmGroupId = vm.getGroupId();
        List<Long> idList = savedAllocation.get(vmGroupId);
        System.out.println(idList);
        List<Host> possibleHosts;
        if(idList==null) {
        	possibleHosts = hostList;
        	System.out.println("Hi");
        }
        else {
        	for(long id : idList) {
            	System.out.println(id);
            }
        	System.out.println("Hi else");
        	System.out.println(idList==NULL);
        	System.out.println(idList.isEmpty());
        	possibleHosts = getHostListExcluding(idList);
        	
        }
        double totalPowerConsumed;
        double minPower = Double.MAX_VALUE;
        int randomChoice;
        boolean flag = true;
        for(int trial = 0;trial<maxIterations;trial++) {
            randomChoice = randInt(0,possibleHosts.size());
            for(int i=0;i<maxNeighborLookup;i++) {
            	currentHost = possibleHosts.get((randomChoice+i)%possibleHosts.size());
            	if(currentHost.isSuitableForVm(vm)) {
            		totalPowerConsumed = getTotalPowerDatacenter(currentHost,vm);
            		if(totalPowerConsumed<minPower) {
            			minPower = totalPowerConsumed;
            			minHost = currentHost;
            			flag = false;
            		}
            	}
            }
        }
        if(flag)
        	return Optional.empty();
        long correspondingHostId = getCorrespondingHostId(minHost);
        if(idList == null) {
        	List<Long> newList = new ArrayList<Long>();
        	newList.add(correspondingHostId);
        	savedAllocation.put(vmGroupId, newList);
        }
        else {
        	savedAllocation.get(vmGroupId).add(correspondingHostId);
        }
        return Optional.of(minHost);
    }
}
