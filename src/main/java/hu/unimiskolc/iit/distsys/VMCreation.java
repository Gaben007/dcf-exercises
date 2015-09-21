package hu.unimiskolc.iit.distsys;

import java.lang.reflect.InvocationTargetException;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine.ResourceAllocation;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.StorageObject;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;

public class VMCreation implements VMCreationApproaches {

	@Override
	public void directVMCreation() throws Exception {
		PhysicalMachine pm = ExercisesBase.getNewPhysicalMachine();
		pm.turnon();
		Timed.simulateUntilLastEvent();
		
		createVMByRequest(pm, "Disk1");
		createVMByRequest(pm, "Disk2");
	}

	@Override
	public void twoPhaseVMCreation() throws Exception {
		PhysicalMachine pm = ExercisesBase.getNewPhysicalMachine();
		pm.turnon();
		Timed.simulateUntilLastEvent();
		
		createVMByDeploy(pm, "Disk1");
		createVMByDeploy(pm, "Disk2");
	}

	@Override
	public void indirectVMCreation() throws Exception {
		PhysicalMachine pm = ExercisesBase.getNewPhysicalMachine();
		Timed.simulateUntilLastEvent();
		
		createVMByIaaSRequest(pm, "Disk1");
		//createVMByIaaSRequest(pm, "Disk1");
	}

	@Override
	public void migratedVMCreation() throws Exception {
		PhysicalMachine pm1 = ExercisesBase.getNewPhysicalMachine();
		pm1.turnon();
		Timed.simulateUntilLastEvent();
		
		PhysicalMachine pm2 = ExercisesBase.getNewPhysicalMachine();
		pm2.turnon();
		Timed.simulateUntilLastEvent();
		
		//createVMByDeploy(pm1, "Disk1");
		createVMByDeploy(pm2, "Disk2");

	}
	
	private void createVMByRequest(PhysicalMachine pm, String storageId) throws VMManagementException, NetworkException{
		VirtualAppliance va = new VirtualAppliance(storageId, 1, 0);
		pm.localDisk.registerObject(va);
		
		ConstantConstraints constraint = new ConstantConstraints(1, 100, 4096);
		
		pm.requestVM(va, constraint, pm.localDisk, 1);
		Timed.simulateUntilLastEvent();
	}
	
	private void createVMByDeploy(PhysicalMachine pm, String storageId) throws VMManagementException, NetworkException{
		VirtualAppliance va = new VirtualAppliance(storageId, 1, 0);
		pm.localDisk.registerObject(va);
		VirtualMachine vm = new VirtualMachine(va);
		
		ConstantConstraints constraint = new ConstantConstraints(1, 100, 4096);
		ResourceAllocation ra = pm.allocateResources(
			constraint, true, 8192
		);
		
		pm.deployVM(vm, ra, pm.localDisk);
		Timed.simulateUntilLastEvent();
	}
	
	private void createVMByIaaSRequest(PhysicalMachine pm, String storageId) throws Exception{
		VirtualAppliance va = new VirtualAppliance(storageId, 1, 0);
		pm.localDisk.registerObject(va);
		
		ConstantConstraints constraint = new ConstantConstraints(1, 100, 4096);
		
		IaaSService iaas = ExercisesBase.getNewIaaSService();
		iaas.registerHost(pm);
		iaas.requestVM(va, constraint, pm.localDisk, 1);
		Timed.simulateUntilLastEvent();
	}
}
