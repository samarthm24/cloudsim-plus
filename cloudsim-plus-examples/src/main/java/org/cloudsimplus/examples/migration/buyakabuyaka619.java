package org.cloudsimplus.examples.migration;

import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationMedianAbsoluteDeviation;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationInterQuartileRange;
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

import java.io.File;

import java.util.*;

public final class buyakabuyaka619 {
    private static final int SCHEDULING_INTERVAL = 60;

    private static final int CLOUDLETS = 1000;
    private static final int CLOUDLET_PES = 2;
    private static final int CLOUDLET_LENGTH = 100000000;

    private static  String TRACE_FILE = "/Users/akash/Desktop/cloudsim-plus-master/cloudsim-plus-examples/src/main/resources/workload/planetlab/20110303/75-130-96-12_static_oxfr_ma_charter_com_irisaple_wup";
    
    private static File[] fileList;
    
    private static final int HOSTS = 500;
    private static final int VMS = 1000;
    private static final long   RACKS = 80;
    private static final long   ISLES = 10;

    private static final int    HOST_MIPS = 3000;//1860; //for each PE
    private static final int    HOST_INITIAL_PES = 2;
    private static final long   HOST_RAM = 160000; //host memory (MB)
    private static final long   HOST_STORAGE = 160000; //host storage
    private static final long   HOST_BW = 4096L; //Mb/s
    private static final double HOST_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION = 0.7;
    
    private static final int    HOST_MIPS_2 = 2660; //for each PE
    private static final int    HOST_INITIAL_PES_2 = 2;
    private static final long   HOST_RAM_2 = 320000; //host memory (MB)
    private static final long   HOST_STORAGE_2 = 320000; //host storage
    private static final long   HOST_BW_2 = 4096L; //Mb/s
 
    private static final int    VM_MIPS =2500 ; //for each PE
    private static final long   VM_SIZE = 1000; //image size (MB)
    private static final int    VM_RAM = 850; //VM memory (MB)
    private static final double VM_BW = 1000;
    private static final int    VM_PES = 1;
    
    private static final int    VM_MIPS_2 = 2000; //for each PE
    private static final long   VM_SIZE_2 = 1000; //image size (MB)
    private static final int    VM_RAM_2 = 3750; //VM memory (MB)
    private static final double VM_BW_2 = 1000;
    private static final int    VM_PES_2 = 1;
    
    private static final int    VM_MIPS_3 = 1000; //for each PE
    private static final long   VM_SIZE_3 = 1000; //image size (MB)
    private static final int    VM_RAM_3 = 1700; //VM memory (MB)
    private static final double VM_BW_3 = 1000;
    private static final int    VM_PES_3 = 1;
    
    private static final int    VM_MIPS_4 = 500; //for each PE
    private static final long   VM_SIZE_4 = 1000; //image size (MB)
    private static final int    VM_RAM_4 = 613; //VM memory (MB)
    private static final double VM_BW_4 = 1000;
    private static final int    VM_PES_4 = 1;

    private static final long   CLOUDLET_LENGHT = 20000;
    private static final long   CLOUDLET_FILESIZE = 300;
    private static final long   CLOUDLET_OUTPUTSIZE = 300;

    private static final double CLOUDLET_INITIAL_CPU_PERCENTAGE = 0.8;


    private static final double CLOUDLET_CPU_INCREMENT_PER_SECOND = 0.1;

    	
    private final List<Vm> vmList = new ArrayList<>();

    private CloudSim simulation;
    private VmAllocationPolicyMigrationInterQuartileRange allocationPolicy;
    private List<Host> hostList;


    public static void main(String[] args) {
    	String dirName = "C:\\Users\\Samarth\\eclipse-workspace\\cloudsim-plus-master\\cloudsim-plus-examples\\src\\main\\resources\\workload\\planetlab\\20110303";
        
        File fileName = new File(dirName);
        fileList = fileName.listFiles();
        
        
        System.out.println("length : " + fileList.length);
        
        for (File file: fileList) {
            
            System.out.println(file.getAbsolutePath());
        }
    	
        new buyakabuyaka619();
    }

    private buyakabuyaka619(){
        

        System.out.println("Starting " + getClass().getSimpleName());
        simulation = new CloudSim(5);

        @SuppressWarnings("unused")
        Datacenter datacenter0 = createDatacenter();
        DatacenterBroker broker = new DatacenterBrokerSimple(simulation);
        createAndSubmitVms(broker);
        createAndSubmitCloudlets(broker);

        simulation.start();

        final List<Cloudlet> finishedList = broker.getCloudletFinishedList();
        finishedList.sort(
            Comparator.comparingLong((Cloudlet c) -> c.getVm().getHost().getId())
                      .thenComparingLong(c -> c.getVm().getId()));
        new CloudletsTableBuilder(finishedList).build();
        System.out.printf("%n    WHEN A HOST CPU ALLOCATED MIPS IS LOWER THAN THE REQUESTED, IT'S DUE TO VM MIGRATION OVERHEAD)%n%n");

        hostList.stream().forEach(this::printHistory);
        System.out.println(getClass().getSimpleName() + " KO!!!");
    }

    private void printHistory(Host host){
        if(printHostStateHistory(host)) {
            printHostCpuUsageAndPowerConsumption(host);
        }
    }


    private boolean printHostStateHistory(Host host) {
        if(host.getStateHistory().stream().anyMatch(HostStateHistoryEntry::isActive)) {
            System.out.printf("%nHost: %6d State History%n", host.getId());
            System.out.println("-------------------------------------------------------------------------------------------");
            host.getStateHistory().forEach(System.out::print);
            System.out.println();
            return true;
        }
        else System.out.printf("Host: %6d was powered off during all the simulation%n", host.getId());
        return false;
    }


    private void printHostCpuUsageAndPowerConsumption(final Host host) {
        System.out.printf("Host: %6d | CPU Usage | Power Consumption in Watt-Second (Ws)%n", host.getId());
        System.out.println("-------------------------------------------------------------------------------------------");
        SortedMap<Double, DoubleSummaryStatistics> utilizationHistory = host.getUtilizationHistory();
        //The total power the Host consumed in the period (in Watt-Sec)
        double totalHostPowerConsumptionWattSec = 0;
        for (Map.Entry<Double, DoubleSummaryStatistics> entry : utilizationHistory.entrySet()) {
            final double time = entry.getKey();
            //The sum of CPU usage of every VM which has run in the Host
            final double hostCpuUsage = entry.getValue().getSum();
            System.out.printf("Time: %6.1f | %9.2f | %.2f%n", time, hostCpuUsage, host.getPowerModel().getPower(hostCpuUsage));
            totalHostPowerConsumptionWattSec += host.getPowerModel().getPower(hostCpuUsage);
        }
        System.out.printf("Total Host power consumption in the period: %.2f Watt-Sec%n", totalHostPowerConsumptionWattSec);
        System.out.println();
    }

    public void createAndSubmitCloudlets(DatacenterBroker broker) {
        List<Cloudlet> list = new ArrayList<>(VMS -1);
        Cloudlet cloudlet = Cloudlet.NULL;
        /*
		 * for(Vm vm: vmList){ cloudlet = createCloudlet(vm, broker, um);
		 * list.add(cloudlet); }
		 */
        list = createCloudlets();
        
        cloudlet.setUtilizationModelCpu(createCpuUtilizationModel(0.2, 1));

        broker.submitCloudletList(list);
    }
    
    private List<Cloudlet> createCloudlets() {
        final List<Cloudlet> list = new ArrayList<>(VMS - 1);
        //final UtilizationModel utilizationCpu = UtilizationModelPlanetLab.getInstance(TRACE_FILE, SCHEDULING_INTERVAL);
        for (int i = 0; i < CLOUDLETS; i++) {
        	
        	TRACE_FILE = fileList[i].getAbsolutePath();
        	final UtilizationModel utilizationCpu = UtilizationModelPlanetLab.getInstance(TRACE_FILE, SCHEDULING_INTERVAL);
            Cloudlet cloudlet =
                new CloudletSimple(i, CLOUDLET_LENGHT, 1)
                    .setFileSize(1024)
                    .setOutputSize(1024)
                    .setUtilizationModelCpu(utilizationCpu)
                    .setUtilizationModelBw(new UtilizationModelDynamic(0.2))
                    .setUtilizationModelRam(new UtilizationModelDynamic(0.4));
            list.add(cloudlet);
        }
        return list;
    }


    public Cloudlet createCloudlet(Vm vm, DatacenterBroker broker, UtilizationModel cpuUtilizationModel) {
        UtilizationModel utilizationModelFull = new UtilizationModelFull();
        final Cloudlet cloudlet =
            new CloudletSimple(CLOUDLET_LENGHT, (int)vm.getNumberOfPes())
                .setFileSize(CLOUDLET_FILESIZE)
                .setOutputSize(CLOUDLET_OUTPUTSIZE)
                .setUtilizationModelCpu(cpuUtilizationModel)
                .setUtilizationModelRam(utilizationModelFull)
                .setUtilizationModelBw(utilizationModelFull);
        broker.bindCloudletToVm(cloudlet, vm);
        return cloudlet;
    }

    public void createAndSubmitVms(DatacenterBroker broker) {
        List<Vm> list = new ArrayList<>(VMS);
        for(int i = 0; i < VMS; i+=4){
            Vm vm = createVm(broker, VM_PES);
            list.add(vm);
            Vm vm2 = createVm2(broker, VM_PES_2);
            list.add(vm2);
            Vm vm3 = createVm3(broker, VM_PES_3);
            list.add(vm3);
            Vm vm4 = createVm4(broker, VM_PES_4);
            list.add(vm4);
            
        }

        vmList.addAll(list);
        broker.submitVmList(list);
    }

    public Vm createVm(DatacenterBroker broker, int pes) {
        Vm vm = new VmSimple(VM_MIPS, pes);
        vm.setGroupId(1);
        vm
          .setRam(VM_RAM).setBw((long)VM_BW).setSize(VM_SIZE)
          .setCloudletScheduler(new CloudletSchedulerTimeShared());
        vm.getUtilizationHistory().enable();
        return vm;
    }
    public Vm createVm2(DatacenterBroker broker, int pes) {
        Vm vm = new VmSimple(VM_MIPS_2, pes);
        vm
          .setRam(VM_RAM_2).setBw((long)VM_BW_2).setSize(VM_SIZE_2)
          .setCloudletScheduler(new CloudletSchedulerTimeShared());
        vm.getUtilizationHistory().enable();
        return vm;
    }
    public Vm createVm3(DatacenterBroker broker, int pes) {
        Vm vm = new VmSimple(VM_MIPS_3, pes);
        vm
          .setRam(VM_RAM_3).setBw((long)VM_BW_3).setSize(VM_SIZE_3)
          .setCloudletScheduler(new CloudletSchedulerTimeShared());
        vm.getUtilizationHistory().enable();
        return vm;
    }
    public Vm createVm4(DatacenterBroker broker, int pes) {
        Vm vm = new VmSimple(VM_MIPS_4, pes);
        vm
          .setRam(VM_RAM_4).setBw((long)VM_BW_4).setSize(VM_SIZE_4)
          .setCloudletScheduler(new CloudletSchedulerTimeShared());
        vm.getUtilizationHistory().enable();
        return vm;
    }


    private UtilizationModelDynamic createCpuUtilizationModel(double initialCpuUsagePercent) {
        return createCpuUtilizationModel(initialCpuUsagePercent, initialCpuUsagePercent);
    }

    private UtilizationModelDynamic createCpuUtilizationModel(double initialCpuUsagePercent, double maxCpuUsagePercentage) {
        if(maxCpuUsagePercentage < initialCpuUsagePercent){
            throw new IllegalArgumentException("Max CPU usage must be equal or greater than the initial CPU usage.");
        }

        initialCpuUsagePercent = Math.min(initialCpuUsagePercent, 1);
        maxCpuUsagePercentage = Math.min(maxCpuUsagePercentage, 1);
        UtilizationModelDynamic um;
        if (initialCpuUsagePercent < maxCpuUsagePercentage) {
            um = new UtilizationModelDynamic(initialCpuUsagePercent)
                        .setUtilizationUpdateFunction(this::getCpuUsageIncrement);
        } else {
            um = new UtilizationModelDynamic(initialCpuUsagePercent);
        }

        um.setMaxResourceUtilization(maxCpuUsagePercentage);
        return um;
    }


    private double getCpuUsageIncrement(final UtilizationModelDynamic um){
        return  um.getUtilization() + um.getTimeSpan()*CLOUDLET_CPU_INCREMENT_PER_SECOND;
    }


    private Datacenter createDatacenter() {
        this.hostList = new ArrayList<>();
        for(int i = 0; i < HOSTS; i=i+2){
            final int pes = HOST_INITIAL_PES;
            final int pes2 = HOST_INITIAL_PES_2;
            Host host = createHost(pes, HOST_MIPS);
            Host host2=createHost2(pes2, HOST_MIPS_2);
            hostList.add(host);
            hostList.add(host2);
        }
        System.out.println();

        final VmAllocationPolicyMigrationStaticThreshold fallback =
            new VmAllocationPolicyMigrationStaticThreshold(
                new VmSelectionPolicyMinimumUtilization(), HOST_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION);


        this.allocationPolicy =
            new VmAllocationPolicyMigrationInterQuartileRange(
                new VmSelectionPolicyMinimumUtilization(),
                HOST_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION+0.2, fallback);

        Datacenter dc = new DatacenterSimple(simulation, hostList, allocationPolicy,RACKS,ISLES);
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        return dc;
    }

    public Host createHost(int numberOfPes, long mipsByPe) {
            List<Pe> peList = createPeList(numberOfPes, mipsByPe);
            Host host =
                new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE, peList);
            host
                .setRamProvisioner(new ResourceProvisionerSimple())
                .setBwProvisioner(new ResourceProvisionerSimple())
                .setVmScheduler(new VmSchedulerTimeShared());
            host.enableStateHistory();
            host.setPowerModel(new PowerModelLinear(50, 0.3));
            return host;
    }
    
    public Host createHost2(int numberOfPes, long mipsByPe) {
        List<Pe> peList = createPeList(numberOfPes, mipsByPe);
        Host host =
            new HostSimple(HOST_RAM_2, HOST_BW_2, HOST_STORAGE_2, peList);
        host
            .setRamProvisioner(new ResourceProvisionerSimple())
            .setBwProvisioner(new ResourceProvisionerSimple())
            .setVmScheduler(new VmSchedulerTimeShared());
        host.enableStateHistory();
        host.setPowerModel(new PowerModelLinear(50, 0.3));
        return host;
}

    public List<Pe> createPeList(int numberOfPEs, long mips) {
        List<Pe> list = new ArrayList<>(numberOfPEs);
        for(int i = 0; i < numberOfPEs; i++) {
            list.add(new PeSimple(mips, new PeProvisionerSimple()));
        }
        return list;
    }
}
