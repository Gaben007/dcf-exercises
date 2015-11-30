package hu.unimiskolc.iit.distsys;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
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
			VaRepoContainer vaRepoPair = getVA(iaas);
			if (vaRepoPair == null)
				throw new Exception("VA is not registered in the IaaS");
						
			for (PhysicalMachine pm : iaas.machines){
				if (!pm.isRunning()){
					pm.turnon();
				}
			}
			Timed.simulateUntilLastEvent();
			
			for (PhysicalMachine pm : iaas.machines){
				double allFreeCapacity = pm.freeCapacities.getRequiredCPUs();
				System.out.println(allFreeCapacity);
			}
			
			System.out.println("-------------");
			
			createVirtualMachines(iaas, vaRepoPair, vmCount);
			Timed.simulateUntilLastEvent();
			
			for (PhysicalMachine pm : iaas.machines){
				double allFreeCapacity = pm.freeCapacities.getRequiredCPUs();
				System.out.println(allFreeCapacity);
			}
		}
		catch (Exception e) { }
	}

	private void createVirtualMachines(IaaSService iaas, VaRepoContainer vaRepoPair, int vmCount) throws Exception {
		//double pmsCount = iaas.machines.size();
		double createdVmCount = 0;
		
		while (createdVmCount < vmCount){
			
			double neededVmsCount = vmCount - createdVmCount;
			
			if (neededVmsCount > 10){
				double allCpu = getAllCpu(iaas);
				double maxCpu = getMaxCpu(iaas);
				double nextCpuSize = allCpu / neededVmsCount * 0.8;
				
				nextCpuSize = nextCpuSize > maxCpu ? maxCpu : nextCpuSize;
				PhysicalMachine minPm = getMinimalRequiredPm(iaas, nextCpuSize);
				double ratio = nextCpuSize / minPm.freeCapacities.getRequiredCPUs();
				double nextPower = minPm.freeCapacities.getRequiredProcessingPower() * ratio;
				
				VirtualMachine vms[] = null;
				try {
					vms = iaas.requestVM(vaRepoPair.getVa(), new ConstantConstraints(nextCpuSize, nextPower / nextCpuSize, 10), vaRepoPair.getRepository(), 1);
					Timed.simulateUntilLastEvent();
				}
				catch (Exception e) {
				}
				
				if (!(vms == null || vms.length == 0 || vms[0] == null || vms[0].getState() != State.RUNNING)) {
					createdVmCount++;
				}
			}
			else {
				PhysicalMachine nextPm = getNextMaxFreePm(iaas);
				if (nextPm == null){
					vmCount--;
					continue;
				}
				
				VirtualMachine vms[] = null;
				try {
					vms = iaas.requestVM(vaRepoPair.getVa(),
							new ConstantConstraints(
									nextPm.freeCapacities.getRequiredCPUs() * nextPm.getCapacities() .getRequiredProcessingPower() / nextPm.freeCapacities .getRequiredProcessingPower(),
									nextPm.freeCapacities.getRequiredProcessingPower(),
									nextPm.freeCapacities.getRequiredMemory()), vaRepoPair.getRepository(),
							1);
					Timed.simulateUntilLastEvent();
				}
				catch (Exception e) {
				}
				
				if (!(vms == null || vms.length == 0 || vms[0] == null || vms[0].getState() != State.RUNNING)) {
					createdVmCount++;
				}
				else {
					vmCount--;
				}
			}
		}
		
		System.out.println(createdVmCount);
	}
	
	private PhysicalMachine getNextMaxFreePm(IaaSService iaas) {
		/*double maxPower = getMaxPower(iaas);
		
		for (PhysicalMachine pm : iaas.machines) {
			if (pm.freeCapacities.getTotalProcessingPower() >= maxPower) {
				return pm;
			}
		}
		
		return null;*/
		
		
		double maxCpu = getMaxCpu(iaas);
		
		for (PhysicalMachine pm : iaas.machines) {
			if (pm.freeCapacities.getRequiredCPUs() >= maxCpu) {
				return pm;
			}
		}
		
		return null;
	}
	
	private PhysicalMachine getMinimalRequiredPm(IaaSService iaas, double minCpu) {
		PhysicalMachine pmResult = null;
		
		for (PhysicalMachine pm : iaas.machines) {
			if (pm.freeCapacities.getRequiredCPUs() >= minCpu) {
				if (pmResult == null) {
					pmResult = pm;
				}
				else {
					if (pmResult.freeCapacities.getRequiredCPUs() > pm.freeCapacities.getRequiredCPUs()) {
						pmResult = pm;	
					}
				}
			}
		}
		
		return pmResult;
	}
	
	private double getAllCpu(IaaSService iaas) {
		double result = 0;
		
		for (PhysicalMachine pm : iaas.machines) {
			result += pm.freeCapacities.getRequiredCPUs();
		}
		
		return result;
	}
	
	private double getMaxCpu(IaaSService iaas) {
		double result = 0;
		
		for (PhysicalMachine pm : iaas.machines) {
			if (result < pm.freeCapacities.getRequiredCPUs()){
				result = pm.freeCapacities.getRequiredCPUs();
			}
		}
		
		return result;
	}
	
	private double getAllPower(IaaSService iaas) {
		double result = 0;
		
		for (PhysicalMachine pm : iaas.machines) {
			result += pm.freeCapacities.getTotalProcessingPower();
		}
		
		return result;
	}
	
	private double getMaxPower(IaaSService iaas) {
		double result = 0;
		
		for (PhysicalMachine pm : iaas.machines) {
			if (result < pm.freeCapacities.getTotalProcessingPower())
				result = pm.freeCapacities.getTotalProcessingPower();
		}
		
		return result;
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
