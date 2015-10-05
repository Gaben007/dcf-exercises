package hu.unimiskolc.iit.distsys;

import java.awt.List;
import java.util.ArrayList;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.StorageObject;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.interfaces.FillInAllPMs;

public class PhysicalMachineFiller implements FillInAllPMs {

	@Override
	public void filler(IaaSService iaas, int vmCount) {
		try {
			for (PhysicalMachine pm : iaas.machines){
				if (!pm.isRunning()){
					pm.turnon();
				}
			}
			Timed.simulateUntilLastEvent();
			
			createVirtualMachines(iaas, vmCount);
		}
		catch (Exception e) { }
	}

	private void createVirtualMachines(IaaSService iaas, int vmCount) throws Exception {
		VaRepoContainer vaRepoPair = getVA(iaas);
		if (vaRepoPair == null)
			throw new Exception("VA is not registered in the IaaS");

		int vmCountPerPm = vmCount / iaas.machines.size();
		
		for (PhysicalMachine pm : iaas.machines){
			ResourceConstraints pmResources = pm.getCapacities();
			double processorCountPerVm = pm.freeCapacities.getRequiredCPUs() / vmCountPerPm;
			
			ResourceConstraints rc = new ConstantConstraints(processorCountPerVm, pmResources.getRequiredProcessingPower() / vmCountPerPm, pm.freeCapacities.getRequiredMemory() / vmCountPerPm);
			requestSpecifiedVM(iaas, vaRepoPair.getVa(), rc, vaRepoPair.getRepository(), vmCountPerPm);
			Timed.simulateUntilLastEvent();
		}
		
		
	}
	
	private VaRepoContainer getVA(IaaSService iaas) throws Exception {
		for (Repository repo : iaas.repositories){
			for (StorageObject so : repo.contents()) {
				if (so instanceof VirtualAppliance)
					return new VaRepoContainer(repo, (VirtualAppliance)so);
			}
		}
		
		return null;
	}
	
	private VirtualMachine[] requestSpecifiedVM(IaaSService iaas, VirtualAppliance va, ResourceConstraints rc, Repository repo, int count) throws Exception{
		return iaas.requestVM(va, rc, repo, count);
	}
	
	private class VmRequester {
		private ArrayList<VmRequest> requests;
		
		public VmRequester() {
			this.requests = new ArrayList<VmRequest>();
		}
	}
	
	private class VmRequest{
		ResourceConstraints rc;
		int count;
		
		public VmRequest(ResourceConstraints rc, int count){
			this.rc = rc;
			this.count = count;
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
}
