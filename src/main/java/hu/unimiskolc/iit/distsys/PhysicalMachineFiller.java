package hu.unimiskolc.iit.distsys;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

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
		
		
		VmRequester vmCreationRequests = new VmRequester();
		for (PhysicalMachine pm : iaas.machines){
			ResourceConstraints pmResources = pm.getCapacities();
			double processorCountPerVm = pm.freeCapacities.getRequiredCPUs() / vmCountPerPm;
			
			ResourceConstraints rc = new ConstantConstraints(processorCountPerVm, pmResources.getRequiredProcessingPower() / vmCountPerPm, pm.freeCapacities.getRequiredMemory() / vmCountPerPm);
			vmCreationRequests.addRequest(new VmRequest(rc, vmCountPerPm));
		}
		
		requestSpecifiedVM(iaas, vaRepoPair, vmCreationRequests);
		Timed.simulateUntilLastEvent();
		
		for (PhysicalMachine pm : iaas.machines){
			double alma = pm.freeCapacities.getRequiredCPUs();
			//java.io.Console
			System.out.println(alma);
			alma += 1;
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
	
	private ArrayList<VirtualMachine> requestSpecifiedVM(IaaSService iaas, VaRepoContainer vaRepoPair, VmRequester vmCreationRequests)throws Exception{
		ArrayList<VirtualMachine> results = new ArrayList<VirtualMachine>();
		
		for (VmRequest request : vmCreationRequests.getOrderedRequests()){
			results.addAll(Arrays.asList(iaas.requestVM(vaRepoPair.getVa(), request.rc, vaRepoPair.getRepository(), request.count)));
		}
		
		return results;
	}
	
	
	// Private methods
	private class VmRequester {
		private ArrayList<VmRequest> requests;
		
		public VmRequester() {
			this.requests = new ArrayList<VmRequest>();
		}

		public void addRequest(VmRequest request) {
			this.requests.add(request);
		}
		
		public ArrayList<VmRequest> getOrderedRequests() {
			ArrayList<VmRequest> result = (ArrayList<VmRequest>)this.requests.clone();
			Collections.sort(result, Collections.reverseOrder());
			return result;
		}
	}
	
	private class VmRequest implements Comparable<VmRequest>{
		ResourceConstraints rc;
		int count;
		
		public VmRequest(ResourceConstraints rc, int count){
			this.rc = rc;
			this.count = count;
		}

		@Override
		public int compareTo(VmRequest arg0) {
			double sCpu = this.rc.getRequiredCPUs();
			double tCpu = arg0.rc.getRequiredCPUs();
			
			if (sCpu == tCpu)
				return 0;
			if (sCpu > tCpu)
				return 1;
			return -1;
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
