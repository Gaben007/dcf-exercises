package hu.unimiskolc.iit.distsys;

import java.util.ArrayList;
import java.util.Collection;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
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
			this.vms.add(new VmContainer(vm));
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
		while (vmc == null)
			vmc = getIdealVm();
		
		try{
			job.startNowOnVM(vmc.vm, new FinishedJobEventHandler(vmc));
		}
		catch (Exception e)
		{ }
	}

	
	
	private VmContainer getIdealVm(){
		for (VmContainer vmc : this.vms) {
			if (!vmc.isProcessingJob){
				vmc.isProcessingJob = true;
				return vmc;
			}
		}
		
		// create a new vm and store in vms
		VaRepoContainer vaRepo = this.getVA();
		ResourceConstraints availableRc = getFreePhysicalMachineCapacity();
		ConstantConstraints rc = new ConstantConstraints(availableRc.getRequiredCPUs() / 5, availableRc.getTotalProcessingPower() / 5, availableRc.getRequiredMemory() / 5);
		
		VirtualMachine[] createdVms;
		try{
			createdVms = iaas.requestVM(vaRepo.getVa(), rc, vaRepo.getRepository(), 1);
		}
		catch (Exception e) {
			return null;
		}
		
		Timed.simulateUntilLastEvent();
		if (createdVms == null || createdVms.length == 0 || createdVms[0] == null || createdVms[0].getState() != State.RUNNING)
			return null; // there are no enough resource
		
		
		
		// retrieves new vmc
		VmContainer vmc = new VmContainer(createdVms[0], true);
		this.vms.add(vmc) ;
		return vmc;
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
	
	
	public class VmContainer {
		public VirtualMachine vm;
		public boolean isProcessingJob;
		
		public VmContainer(VirtualMachine vm) {
			this.vm = vm;
			this.isProcessingJob = false;
		}
		
		public VmContainer(VirtualMachine vm, boolean isProcessingJob) {
			this.vm = vm;
			this.isProcessingJob = isProcessingJob;
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
}












































