package hu.unimiskolc.iit.distsys;

import java.util.ArrayList;
import java.util.Collection;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.DCFJob;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.StateChange;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.StorageObject;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

public class HaRRJSched implements BasicJobScheduler {
	public static int handleCallCount = 0;
	public static int retryRequestCount = 0;
	public static int delayedCreationCount = 0;
	public static int unsuccessfulCreationCount = 0;
	public static int nullResourceFound = 0;
	
	private IaaSService iaas;
	private double timeRatio = 0.8;
	Collection<VmContainer> vms = new ArrayList<VmContainer>();
	
	@Override
	public void setupVMset(Collection<VirtualMachine> vms) {
		for (VirtualMachine vm : vms) {
			this.vms.add(new VmContainer(vm, false));
		}
	}

	@Override
	public void setupIaaS(IaaSService iaas) {
		this.iaas = iaas;
	}

	@Override
	public void handleJobRequestArrival(Job j) {
		if (this.iaas == null)
			return; // focus only the exact problem
		
		ComplexDCFJob job = null;
		if (j instanceof ComplexDCFJob)
			job = (ComplexDCFJob)j;
		
		if (job == null)
			return; // handle only ComplexDCFJobs
		
		// start VM finding
		VmContainer vmc = null;
		vmc = getOrCreateAndStartIdealVmWithJob(job);
		
		
		if (vmc == null)
			return;
		
		handleCallCount++;
		try {
			job.startNowOnVM(vmc.vm, new ConsumptionEventHandler(this, job, vmc));
			//System.out.println(index++ + " VM reuse (" + job.getId() + ")");
			
			vmc.currentStateHandler = new VmStateChangedHandler(this, job, vmc);
			vmc.vm.subscribeStateChange(vmc.currentStateHandler);
		}
		catch (Exception e)
		{ }
	}
	
	
	private VmContainer getOrCreateAndStartIdealVmWithJob(ComplexDCFJob job){
		for (VmContainer vmc : this.vms) {
			if (!vmc.getIsProcessingJob() && vmc.resource.getTotalProcessingPower() > job.nprocs * ExercisesBase.maxProcessingCap * timeRatio) {
				vmc.setIsProcessingJob(true);
				return vmc;
			}
		}
		
		// create a new vm and store in vms
		VaRepoContainer vaRepo = this.getVA();
		//ResourceConstraints availableRc = getFreePhysicalMachineCapacity();
		//ConstantConstraints rc = new ConstantConstraints(availableRc.getRequiredCPUs() / 5, availableRc.getRequiredProcessingPower() / 5, availableRc.getRequiredMemory() / 5);
		double requiredResource = job.nprocs * ExercisesBase.maxProcessingCap;
		double requiredProcNum;
		ResourceConstraints availableEnoughRc = getMinimalAvailablePhysicalResource(requiredResource);
		if (availableEnoughRc == null) {
			nullResourceFound++;
			//return null;
			
			//Timed.jumpTime(1000);
			//this.handleJobRequestArrival(job);
			return null;
		}
		else {
			requiredProcNum = requiredResource / availableEnoughRc.getRequiredProcessingPower();
			if (requiredProcNum > availableEnoughRc.getRequiredCPUs())
				requiredProcNum = availableEnoughRc.getRequiredCPUs();
		}
		
		ConstantConstraints rc = new ConstantConstraints(
			//requiredProcNum,
			availableEnoughRc.getRequiredCPUs(),
			availableEnoughRc.getRequiredProcessingPower(),
			1
		);
		
		VirtualMachine[] createdVms;
		try {
			createdVms = iaas.requestVM(vaRepo.getVa(), rc, vaRepo.getRepository(), 1);
		}
		catch (Exception e) {
			System.out.println("Error: 1");
			return null;
		}
		
		//Timed.simulateUntilLastEvent();
		if (createdVms == null || createdVms.length == 0 || createdVms[0] == null) {
			System.out.println("Error: 2");
			return null; // there are no enough resource
		}
		
		VirtualMachine createdVm = createdVms[0];
		if (createdVm.getState() == State.RUNNING)
			return new VmContainer(createdVm, true);
		
		createdVm.subscribeStateChange(new VmCreationHandler(this, job, rc));
		
		/*if (createdVm.getState() == State.INITIAL_TR)
			// wait for VM creation
			
		else {
			// terminate unsuccessfully created VM
			unsuccessfulCreationCount++;
			
			if (createdVm.getState() == State.DESTROYED){
				try {
					iaas.terminateVM(createdVm, true);
				}
				catch (Exception e)
				{ }
			}
			
			// try to create again
			//this.handleJobRequestArrival(job);
		}
		*/
		
		return null;
	}
	
	private VaRepoContainer getVA() {
		for (Repository repo : this.iaas.repositories){
			for (StorageObject so : repo.contents()) {
				if (so instanceof VirtualAppliance)
					return new VaRepoContainer(repo, (VirtualAppliance)so);
			}
		}
		
		return null;
	}
	
	private ResourceConstraints  getFreePhysicalMachineCapacity(){
		for (PhysicalMachine pm : this.iaas.machines){
			if (pm.freeCapacities.getRequiredCPUs() > 1 && pm.freeCapacities.getRequiredMemory() > 10 && pm.freeCapacities.getRequiredProcessingPower() > 10)
				return pm.freeCapacities;
		}
		
		return null;
	}
	
	private ResourceConstraints getMinimalAvailablePhysicalResource(double requestedResource) {
		ResourceConstraints resultRc = null;
		
		for (PhysicalMachine pm : this.iaas.machines) {
			if (pm.freeCapacities.getTotalProcessingPower() >= requestedResource * timeRatio && (resultRc == null || resultRc.getTotalProcessingPower() > pm.freeCapacities.getTotalProcessingPower()))
				resultRc = pm.freeCapacities;
				//return pm.freeCapacities;
		}
		
		return resultRc;
	}
	
	
	
	public class VmContainer {
		private boolean isProcessingJob;
		private HaRRJSched scheduler;
		
		public VirtualMachine vm;
		public ResourceConstraints resource;
		public VmStateChangedHandler currentStateHandler;
		
		public VmContainer(VirtualMachine vm, ResourceConstraints rc, HaRRJSched scheduler) {
			this.vm = vm;
			this.resource = rc;
			this.scheduler = scheduler;
			this.isProcessingJob = false;
		}
		
		public VmContainer(VirtualMachine vm, boolean isProcessingJob) {
			this.vm = vm;
			this.isProcessingJob = isProcessingJob;
		}
		
		public boolean getIsProcessingJob() {
			return this.isProcessingJob;
		}
		
		public void setIsProcessingJob(boolean value) {
			this.isProcessingJob = value;
		}
	}
	
	private class VaRepoContainer {
		private Repository repo;
		private VirtualAppliance va;
		
		public VaRepoContainer(Repository repository, VirtualAppliance virtualAppliance){
			this.repo = repository;
			this.va = virtualAppliance;
		}
		
		public Repository getRepository() {
			return this.repo;
		}
		
		public VirtualAppliance getVa() {
			return this.va;
		}
	}

	private class VmCreationHandler implements StateChange {
		private HaRRJSched scheduler;
		private ComplexDCFJob job;
		private ResourceConstraints rc;
		
		public VmCreationHandler(HaRRJSched scheduler, ComplexDCFJob job, ResourceConstraints rc) {
			this.scheduler = scheduler;
			this.job = job;
			this.rc = rc;
		}
		
		@Override
		public void stateChanged(VirtualMachine vm, State oldState, State newState) {
			if (newState == State.RUNNING){

				delayedCreationCount++;
				// retrieves new vmc
				VmContainer vmc = new VmContainer(vm, this.rc, this.scheduler);
				this.scheduler.vms.add(vmc);
				
				try {
					this.job.startNowOnVM(vmc.vm, new ConsumptionEventHandler(this.scheduler, this.job, vmc));
					//System.out.println(index++ + " VM creation (" + job.getId() + ")");
					
					vm.unsubscribeStateChange(this);
					vmc.currentStateHandler = new VmStateChangedHandler(this.scheduler, this.job, vmc);
					vm.subscribeStateChange(vmc.currentStateHandler);
				}
				catch (Exception e)
				{ }
			}
			else if (newState == State.DESTROYED) {
				//System.out.println(index + " VM recreation (" + job.getId() + ") ---------------------");
				retryRequestCount++;
				this.scheduler.handleJobRequestArrival(this.job);
			}
		}
	}

	private class VmStateChangedHandler implements StateChange {
		private HaRRJSched scheduler;
		private ComplexDCFJob job;
		private VmContainer vmc;
		
		public boolean needToHandle = true;
		
		public VmStateChangedHandler(HaRRJSched scheduler, ComplexDCFJob job, VmContainer vmc) {
			this.scheduler = scheduler;
			this.job = job;
			this.vmc = vmc;
		}
		
		@Override
		public void stateChanged(VirtualMachine vm, State oldState, State newState) {
			if (needToHandle) {
				if (oldState == State.RUNNING && newState == State.DESTROYED) {
					this.scheduler.vms.remove(vmc);
				}
			}
		}
	}
	
	private class ConsumptionEventHandler implements ConsumptionEvent {
		private HaRRJSched scheduler;
		private ComplexDCFJob job;
		private VmContainer vmc;
		
		public ConsumptionEventHandler(HaRRJSched scheduler, ComplexDCFJob job, VmContainer vmc) {
			this.scheduler = scheduler;
			this.job = job;
			this.vmc = vmc;
		}
		
		@Override
		public void conComplete() {
			this.vmc.setIsProcessingJob(false);
		}

		@Override
		public void conCancelled(ResourceConsumption problematic) {
			this.scheduler.handleJobRequestArrival(new ComplexDCFJob(this.job));
		}
	}
}



























































