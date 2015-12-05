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
			
			System.out.println("-------------");
			
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
			
			if (neededVmsCount > iaas.machines.size()){
				double allCpu = getAllCpu(iaas);
				double maxCpu = getMaxCpu(iaas);
				double nextCpuSize = allCpu / neededVmsCount * 0.8;
				
				nextCpuSize = nextCpuSize > maxCpu ? maxCpu : nextCpuSize;
				PhysicalMachine minPm = getMinimalRequiredPm(iaas, nextCpuSize);
				double ratio = nextCpuSize / minPm.freeCapacities.getRequiredCPUs();
				double nextPower = minPm.freeCapacities.getRequiredProcessingPower() * ratio;
				
				VirtualMachine vms[] = null;
				try {
					vms = iaas.requestVM(vaRepoPair.getVa(),
							new ConstantConstraints(
								nextCpuSize * 0.99,
								nextPower / nextCpuSize,
								10),
							vaRepoPair.getRepository(), 1);
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
					System.out.println("There is no enough PM.");
					System.out.println(createdVmCount);
					return;
				}
				
				VirtualMachine vms[] = null;
				double ratio = nextPm.getCapacities().getRequiredProcessingPower() / nextPm.freeCapacities.getRequiredProcessingPower();
				
				for (int i = 0; i < 10; i++)
				{
					try {
						vms = iaas.requestVM(vaRepoPair.getVa(),
								new ConstantConstraints(
										nextPm.freeCapacities.getRequiredCPUs() * ratio,
										nextPm.freeCapacities.getRequiredProcessingPower() * ratio,
										nextPm.freeCapacities.getRequiredMemory() / 2
								),
								vaRepoPair.getRepository(), 1);
						
						Timed.simulateUntilLastEvent();
					}
					catch (Exception e) {
					}
					
					if (!(vms == null || vms.length == 0 || vms[0] == null || vms[0].getState() != State.RUNNING)) {
						createdVmCount++;
						break;
					}
					else {
						System.out.println(i + " VM creation was failed.");
						System.out.println(nextPm.freeCapacities.getRequiredCPUs());
						System.out.println(nextPm.freeCapacities.getRequiredCPUs() * ratio);
						System.out.println(ratio);
						System.out.println(nextPm.getCapacities().getRequiredProcessingPower() / nextPm.freeCapacities.getRequiredProcessingPower());
						if (i == 9)
							vmCount--;
						else
							ratio = ratio <= 1 ? ratio * 0.99 : 1.0;
					}
				}
			}
		}
		
		System.out.println("Created: " + createdVmCount);
	}
	
	private PhysicalMachine getNextMaxFreePm(IaaSService iaas) {
		double maxPower = getMaxPower(iaas);
		
		for (PhysicalMachine pm : iaas.machines) {
			if (pm.freeCapacities.getTotalProcessingPower() >= maxPower) {
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
		double result = -1;
		
		for (PhysicalMachine pm : iaas.machines) {
			if (result < pm.freeCapacities.getRequiredCPUs()){
				result = pm.freeCapacities.getRequiredCPUs();
			}
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
	
	
	// Private methods
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
