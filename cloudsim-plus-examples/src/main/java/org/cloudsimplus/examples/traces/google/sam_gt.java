/*
 * CloudSim Plus: A modern, highly-extensible and easier-to-use Framework for
 * Modeling and Simulation of Cloud Computing Infrastructures and Services.
 * http://cloudsimplus.org
 *
 *     Copyright (C) 2015-2018 Universidade da Beira Interior (UBI, Portugal) and
 *     the Instituto Federal de Educação Ciência e Tecnologia do Tocantins (IFTO, Brazil).
 *
 *     This file is part of CloudSim Plus.
 *
 *     CloudSim Plus is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     CloudSim Plus is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with CloudSim Plus. If not, see <http://www.gnu.org/licenses/>.
 */
package org.cloudsimplus.examples.traces.google;

import ch.qos.logback.classic.Level;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationMedianAbsoluteDeviation;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationStaticThreshold;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.power.models.PowerModelLinear;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisioner;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmScheduler;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicyMinimumUtilization;
import org.cloudbus.cloudsim.util.Conversion;
import org.cloudbus.cloudsim.util.TimeUtil;
import org.cloudbus.cloudsim.util.TraceReaderAbstract;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.TextTableColumn;
import org.cloudsimplus.traces.google.GoogleTaskEventsTraceReader;
import org.cloudsimplus.traces.google.GoogleTaskUsageTraceReader;
import org.cloudsimplus.traces.google.TaskEvent;
import org.cloudsimplus.util.Log;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.cloudbus.cloudsim.utilizationmodels.UtilizationModel.Unit;

/**
 * An example showing how to create Cloudlets (tasks) from a Google Task Events
 * Trace using a {@link GoogleTaskEventsTraceReader}. Then it uses a
 * {@link GoogleTaskUsageTraceReader} to read "task usage" trace files that
 * define how the created Cloudlets will use resources along the time.
 *
 * <p>
 * The trace are located in resources/workload/google-traces/. Each line in the
 * "task events" trace defines the scheduling of tasks (Cloudlets) inside a
 * Datacenter.
 * </p>
 *
 * <p>
 * Check important details at {@link TraceReaderAbstract}. To better understand
 * the structure of trace files, check the google-cluster-data-samples.xlsx
 * spreadsheet inside the docs dir.
 * </p>
 *
 * @author Manoel Campos da Silva Filho
 * @since CloudSim Plus 4.0.0
 *
 * @TODO A joint example that creates Hosts and Cloudlets from trace files will
 *       be useful.
 *
 * @TODO See https://github.com/manoelcampos/cloudsim-plus/issues/151
 * @TODO {@link CloudSimTags#CLOUDLET_FAIL} events aren't been processed.
 * @TODO It has to be checked how to make the Cloudlet to be executed in the
 *       Host specified in the trace file.
 */
public class sam_gt {
    private static final String TRACE_FILENAME = "workload/google-traces/task-events-sample-1.csv";

    private static final int SCHEDULING_INTERVAL = 300;

    private static final int CLOUDLETS = 1;
    private static final int CLOUDLET_PES = 2;
    //private static final int CLOUDLET_LENGTH = 100000000;

    private static final String TRACE_FILE = "workload/planetlab/20110303/75-130-96-12_static_oxfr_ma_charter_com_irisaple_wup";
   
    private static final int HOSTS = 800;
    private static final int VMS = 1000;

    private static final int    HOST_MIPS = 1860; //for each PE
    private static final int    HOST_INITIAL_PES = 2;
    private static final long   HOST_RAM = 160000; //host memory (MB)
    private static final long   HOST_STORAGE = 160000; //host storage
    private static final long   HOST_BW = 1024L; //Mb/s
    private static final double HOST_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION = 0.7;
   
    private static final int    HOST_MIPS_2 = 2660; //for each PE
    private static final int    HOST_INITIAL_PES_2 = 2;
    private static final long   HOST_RAM_2 = 32000; //host memory (MB)
    private static final long   HOST_STORAGE_2 = 320000; //host storage
    private static final long   HOST_BW_2 = 1024L; //Mb/s
 
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

    private static final long   CLOUDLET_LENGTH = 20000;
    private static final long   CLOUDLET_FILESIZE = 300;
    private static final long   CLOUDLET_OUTPUTSIZE = 300;

    private static final double CLOUDLET_INITIAL_CPU_PERCENTAGE = 0.8;


    private static final double CLOUDLET_CPU_INCREMENT_PER_SECOND = 0.1;

    private final CloudSim simulation;
    private VmAllocationPolicyMigrationMedianAbsoluteDeviation allocationPolicy;
    private List<Host> hostList;
    private final List<Vm> vmList = new ArrayList<>();
    private List<DatacenterBroker> brokers;
    private Datacenter datacenter;
    private Set<Cloudlet> cloudlets;

    public static void main(String[] args) {
        new sam_gt();
    }

    private sam_gt() {
        final double startSecs = TimeUtil.currentTimeSecs();
        System.out.printf("Simulation started at %s%n%n", LocalTime.now());
        Log.setLevel(Level.TRACE);

        simulation = new CloudSim();
        datacenter = createDatacenter();

        createCloudletsAndBrokersFromTraceFile();
        brokers.forEach(broker -> createAndSubmitVms(broker));
        readTaskUsageTraceFile();

        simulation.start();

        brokers.forEach(this::printCloudlets);
        System.out.printf("Simulation finished at %s. Execution time: %.2f seconds%n", LocalTime.now(), TimeUtil.elapsedSeconds(startSecs));
    }

    /**
     * Creates a list of Cloudlets from a "task events" Google Cluster Data trace file.
     * The brokers that own each Cloudlet are defined by the username field
     * in the file. For each distinct username, a broker is created
     * and its name is defined based on the username.
     *
     * <p>
     * A {@link GoogleTaskEventsTraceReader} instance is used to read the file.
     * It requires a {@link Function}
     * that will be called internally to actually create the Cloudlets.
     * This function is the {@link #createCloudlet(TaskEvent)}.*
     * </p>
     */
    private void createCloudletsAndBrokersFromTraceFile() {
        final GoogleTaskEventsTraceReader reader =
            GoogleTaskEventsTraceReader.getInstance(simulation, TRACE_FILENAME, this::createCloudlet);

        /*The created Cloudlets are automatically submitted to their respective brokers,
        so you don't have to submit them manually.*/
        cloudlets = reader.process();
        brokers = reader.getBrokers();
        System.out.printf("%d Cloudlets and %d Brokers created from the %s trace file.%s",
            cloudlets.size(), brokers.size(), TRACE_FILENAME, System.lineSeparator());
    }

    /**
     * A method that is used to actually create each Cloudlet defined as a task in the
     * trace file.
     * The researcher can write his/her own code inside this method to define
     * how he/she wants to create the Cloudlet based on the trace data.
     *
     * @param event an object containing the trace line read, used to create the Cloudlet.
     * @return
     */
    private Cloudlet createCloudlet(final TaskEvent event) {
        /*
        The trace doesn't define the actual number of CPU cores (PEs) a Cloudlet will require,
        but just a percentage of the number of cores that is required.
        This way, we have to compute the actual number of cores.
        This is different from the CPU UtilizationModel, which is defined
        in the "task usage" trace files.
        */
        final long pesNumber = event.actualCpuCores(VM_PES) > 0 ? event.actualCpuCores(VM_PES) : VM_PES;

        final double maxRamUsagePercent = event.getResourceRequestForRam() > 0 ? event.getResourceRequestForRam() : Conversion.HUNDRED_PERCENT;
        final UtilizationModelDynamic utilizationRam = new UtilizationModelDynamic(Unit.PERCENTAGE, 0, maxRamUsagePercent);

        final long sizeInBytes = (long) Math.ceil(Conversion.megaBytesToBytes(event.getResourceRequestForLocalDiskSpace()*VM_SIZE + 1));
        return new CloudletSimple(CLOUDLET_LENGTH, pesNumber)
            .setFileSize(sizeInBytes)
            .setOutputSize(sizeInBytes)
            .setUtilizationModelBw(new UtilizationModelFull())
            .setUtilizationModelCpu(new UtilizationModelFull())
            .setUtilizationModelRam(utilizationRam);
    }


    /**
     * Process a "task usage" trace file from the Google Cluster Data that
     * defines the resource usage for Cloudlets (tasks) along the time.
     * The reader is just considering data about RAM and CPU utilization.
     *
     * <p>
     * You are encouraged to check {@link GoogleTaskUsageTraceReader#process()}
     * documentation to understand the details.
     * </p>
     */
    private void readTaskUsageTraceFile() {
        final String fileName = "workload/google-traces/task-usage-sample-1.csv";
        final GoogleTaskUsageTraceReader reader =
            GoogleTaskUsageTraceReader.getInstance(brokers, fileName);
        final Set<Cloudlet> processedCloudlets = reader.process();
        System.out.printf("%d Cloudlets processed from the %s trace file.%s", processedCloudlets.size(), fileName, System.lineSeparator());
        System.out.println();
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
            new VmAllocationPolicyMigrationMedianAbsoluteDeviation(
                new VmSelectionPolicyMinimumUtilization(),
                HOST_UTILIZATION_THRESHOLD_FOR_VM_MIGRATION+0.2, fallback);

        Datacenter dc = new DatacenterSimple(simulation, hostList, allocationPolicy);
        dc.setSchedulingInterval(SCHEDULING_INTERVAL);
        return dc;
    }

    private long getVmSize(final Cloudlet cloudlet) {
        return cloudlet.getVm().getStorage().getCapacity();
    }

    private long getCloudletSizeInMB(final Cloudlet cloudlet) {
        return (long)Conversion.bytesToMegaBytes(cloudlet.getFileSize());
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

    private List<Pe> createPesList(final int count) {
        final List<Pe> cpuCoresList = new ArrayList<>(count);
        for(int i = 0; i < count; i++){
            cpuCoresList.add(new PeSimple(HOST_MIPS, new PeProvisionerSimple()));
        }

        return cpuCoresList;
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

    private void printCloudlets(final DatacenterBroker broker) {
        final String username = broker.getName().replace("Broker_", "");
        final List<Cloudlet> list = broker.getCloudletFinishedList();
        list.sort(Comparator.comparingLong(Cloudlet::getId));
        new CloudletsTableBuilder(list)
            .addColumn(0, new TextTableColumn("Job", "ID"), Cloudlet::getJobId)
            .addColumn(7, new TextTableColumn("VM Size", "MB"), this::getVmSize)
            .addColumn(8, new TextTableColumn("Cloudlet Size", "MB"), this::getCloudletSizeInMB)
            .addColumn(10, new TextTableColumn("Waiting Time", "Seconds").setFormat("%.0f"), Cloudlet::getWaitingTime)
            .setTitle("Simulation results for Broker representing the username " + username)
            .build();
    }
    public List<Pe> createPeList(int numberOfPEs, long mips) {
        List<Pe> list = new ArrayList<>(numberOfPEs);
        for(int i = 0; i < numberOfPEs; i++) {
            list.add(new PeSimple(mips, new PeProvisionerSimple()));
        }
        return list;
    }
                                           
}