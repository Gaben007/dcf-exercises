package hu.unimiskolc.iit.distsys;

import java.util.ArrayList;
import java.util.Collection;

import javax.naming.spi.DirStateFactory.Result;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.StateChange;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.UnalterableConstraintsPropagator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.StorageObject;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

public class CustomRRJSched implements BasicJobScheduler {
	private IaaSService iaas;
	Collection<VmContainer> vms = new ArrayList<CustomRRJSched.VmContainer>();
	
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
		VmContainer vmc =null;
		int callCounter = 0;
		vmc = getOrCreateAndStartIdealVmWithJob(job);
		
		if (vmc == null)
			return;
		
		try{
			job.startNowOnVM(vmc.vm, new FinishedJobEventHandler(vmc));
		}
		catch (Exception e)
		{ }
	}
	
	public void handleVmDestroyRequest(VmContainer vmc){
		this.vms.remove(vmc);
		//this.iaas.terminateVM(vmc.vm, true);
		
		try {
			vmc.vm.destroy(true);
		}
		catch (Exception e)
		{ }
	}

	
	
	private VmContainer getOrCreateAndStartIdealVmWithJob(ComplexDCFJob job){
		for (VmContainer vmc : this.vms) {
			if (!vmc.getIsProcessingJob()) {
				vmc.setIsProcessingJob(true);
				return vmc;
			}
		}
		
		// create a new vm and store in vms
		VaRepoContainer vaRepo = this.getVA();
		//ResourceConstraints availableRc = getFreePhysicalMachineCapacity();
		//ConstantConstraints rc = new ConstantConstraints(availableRc.getRequiredCPUs() / 5, availableRc.getRequiredProcessingPower() / 5, availableRc.getRequiredMemory() / 5);
		double requiredResource = job.nprocs * ExercisesBase.maxProcessingCap;
		ResourceConstraints availableEnoughRc = getMinimalAvailablePhysicalResource(requiredResource);
		if (availableEnoughRc == null)
			return null;
		
		ConstantConstraints rc = new ConstantConstraints(requiredResource / availableEnoughRc.getRequiredProcessingPower(), availableEnoughRc.getRequiredProcessingPower(), 1);
		
		VirtualMachine[] createdVms;
		try{
			createdVms = iaas.requestVM(vaRepo.getVa(), rc, vaRepo.getRepository(), 1);
		}
		catch (Exception e) {
			return null;
		}
		
		//Timed.simulateUntilLastEvent();
		if (createdVms == null || createdVms.length == 0 || createdVms[0] == null)
			return null; // there are no enough resource
		
		VirtualMachine createdVm = createdVms[0];
		// wait for VM creation
		createdVm.subscribeStateChange(new VmCreationHandler(this, job));
		
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
			if (pm.freeCapacities.getTotalProcessingPower() > requestedResource && (resultRc == null || resultRc.getTotalProcessingPower() > pm.freeCapacities.getTotalProcessingPower()))
				resultRc = pm.freeCapacities;
		}
		
		return resultRc;
	}
	
	
	public class VmContainer {
		private boolean isProcessingJob;
		private DeferredEvent timer;
		private CustomRRJSched scheduler;
		
		public VirtualMachine vm;
		
		public VmContainer(VirtualMachine vm, CustomRRJSched scheduler) {
			this.vm = vm;
			this.scheduler = scheduler;
			this.isProcessingJob = false;
			this.timer = null;
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
			
			if (value)
				stopVmDestroyer();
			else
				startVmDestroyer();
		}
		
		private void startVmDestroyer() {
			this.timer = new CustomDeferredEvent(25000, this.scheduler, this);
		}
		
		private void stopVmDestroyer() {
			// cancels the timer request
			if (this.timer != null) {
				this.timer.cancel();
				this.timer = null;
			}
		}
		
		private class CustomDeferredEvent extends DeferredEvent {
			private CustomRRJSched scheduler;
			private VmContainer vmc;
			
			public CustomDeferredEvent(final long delay, CustomRRJSched scheduler, VmContainer vmc){
				super(delay);
				this.scheduler = scheduler;
				this.vmc = vmc;
			}
			
			@Override
			protected void eventAction() {
				// call an event to destroy the vmc
				this.scheduler.handleVmDestroyRequest(vmc);
			}
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

	private class FinishedJobEventHandler implements ConsumptionEvent  {
		private VmContainer vmc;
		
		public FinishedJobEventHandler(VmContainer vmc) {
			this.vmc = vmc;
		}
		
		@Override
		public void conComplete() {
			this.vmc.isProcessingJob = false;
		}

		@Override
		public void conCancelled(ResourceConsumption problematic) {
			this.vmc.isProcessingJob = false;
		}
	}

	private class VmCreationHandler implements StateChange {
		private CustomRRJSched scheduler;
		private ComplexDCFJob job;
		
		public VmCreationHandler(CustomRRJSched scheduler, ComplexDCFJob job) {
			this.scheduler = scheduler;
			this.job = job;
		}
		
		@Override
		public void stateChanged(VirtualMachine vm, State oldState, State newState) {
			if (newState == State.RUNNING){

				// retrieves new vmc
				VmContainer vmc = new VmContainer(vm, true);
				this.scheduler.vms.add(vmc) ;
				
				try {
					this.job.startNowOnVM(vmc.vm, new FinishedJobEventHandler(vmc));
				}
				catch (Exception e)
				{ }
			}
		}
	}
}












































