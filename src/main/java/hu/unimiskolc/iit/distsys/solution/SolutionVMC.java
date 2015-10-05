/*
 *  ========================================================================
 *  dcf-exercises
 *  ========================================================================
 *  
 *  This file is part of dcf-exercises.
 *  
 *  dcf-exercises is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or (at
 *  your option) any later version.
 *  
 *  dcf-exercises is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with dcf-exercises.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  (C) Copyright 2015, Gabor Kecskemeti (kecskemeti@iit.uni-miskolc.hu)
 */
package hu.unimiskolc.iit.distsys.solution;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine.ResourceAllocation;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.ExercisesBase;
import hu.unimiskolc.iit.distsys.interfaces.VMCreationApproaches;

/**
 * This demonstrator class is showing how to fulfill the requirements of
 * hu.unimiskolc.iit.distsys.TestVMCreation.
 * 
 * To use this class during the test, set the system property called
 * "hu.unimiskolc.iit.distsys.VMC" to
 * "hu.unimiskolc.iit.distsys.solution.SolutionVMC". This can be either achieved
 * by setting command line argument of java with the option
 * <code>-Dhu.unimiskolc.iit.distsys.VMC=hu.unimiskolc.iit.distsys.solution.SolutionVMC</code>
 * , or by setting the system property by calling System.setProperty() before
 * the VMCreatorFactory's createApproachesExcercise() function is called.
 * 
 * @author gaborkecskemeti
 * 
 */
public class SolutionVMC implements VMCreationApproaches {
	/**
	 * Demonstrates how to create two virtual machines directly on a physical
	 * machine with a single function call.
	 * 
	 * For the preparatory steps, the function shows how a virtual appliance is
	 * created and registered to the physical machihne's local disk. During the
	 * preparation the function also shows how to turn on a physical machine.
	 * 
	 * Also demonstrates how to dynamically adjust the requested VM's size to
	 * match the PM that is expected to host the VM.
	 */
	public void directVMCreation() throws Exception {
		PhysicalMachine pm = ExercisesBase.getNewPhysicalMachine();
		pm.turnon();
		Timed.simulateUntilLastEvent();
		VirtualAppliance va = new VirtualAppliance("VAID", 10, 0, false,
				100000000l);
		pm.localDisk.registerObject(va);
		AlterableResourceConstraints caps = new AlterableResourceConstraints(
				pm.getCapacities());
		caps.multiply(0.4);
		ConstantConstraints minCaps = new ConstantConstraints(caps);
		pm.requestVM(va, minCaps, pm.localDisk, 2);
		Timed.simulateUntilLastEvent();
	}

	/**
	 * Reveals how two VMs can be requested from an IaaS service in a single
	 * function call.
	 * 
	 * Demonstrates how a simple IaaS is constructed out of a PM.
	 * 
	 * Reveals that PM state management is not necessary if the IaaS is used for
	 * VM creation.
	 */
	public void indirectVMCreation() throws Exception {
		PhysicalMachine pm = ExercisesBase.getNewPhysicalMachine();
		VirtualAppliance va = new VirtualAppliance("VAID", 10, 0, false,
				100000000l);
		pm.localDisk.registerObject(va);
		AlterableResourceConstraints caps = new AlterableResourceConstraints(
				pm.getCapacities());
		caps.multiply(0.4);
		ConstantConstraints minCaps = new ConstantConstraints(caps);
		IaaSService iaas = ExercisesBase.getNewIaaSService();
		iaas.registerHost(pm);
		iaas.requestVM(va, minCaps, pm.localDisk, 2);
		Timed.simulateUntilLastEvent();
	}

	/**
	 * Demonstrates the use of two physical machines. Shows how to migrate a VM
	 * between the two PMs. Also shows how to set up VM resource demands so it
	 * meets both PM's offered resource set.
	 * 
	 * Shows how the resource allocation for a migration needs to be requested
	 * with a longer expiration length.
	 */
	public void migratedVMCreation() throws Exception {
		PhysicalMachine pmInitial = ExercisesBase.getNewPhysicalMachine();
		PhysicalMachine pmTarget = ExercisesBase.getNewPhysicalMachine();
		pmInitial.turnon();
		pmTarget.turnon();
		Timed.simulateUntilLastEvent();
		VirtualAppliance va = new VirtualAppliance("VAID", 10, 0, false,
				100000000l);
		pmInitial.localDisk.registerObject(va);
		ConstantConstraints migratingCaps = new ConstantConstraints(Math.min(
				pmInitial.getCapacities().getRequiredCPUs(), pmTarget
						.getCapacities().getRequiredCPUs()), Math.min(pmInitial
				.getCapacities().getRequiredProcessingPower(), pmTarget
				.getCapacities().getRequiredProcessingPower()), Math.min(
				pmInitial.getCapacities().getRequiredMemory(), pmTarget
						.getCapacities().getRequiredMemory()));
		VirtualMachine vm = pmInitial.requestVM(va, migratingCaps,
				pmInitial.localDisk, 1)[0];
		Timed.simulateUntilLastEvent();
		ResourceAllocation ra = pmTarget.allocateResources(migratingCaps, true,
				PhysicalMachine.migrationAllocLen * 1000);
		vm.migrate(ra);
		Timed.simulateUntilLastEvent();
	}

	/**
	 * Reveals an alternative VM creation approach on a Physical Machine using
	 * resource allocations.
	 */
	public void twoPhaseVMCreation() throws Exception {
		PhysicalMachine pm = ExercisesBase.getNewPhysicalMachine();
		pm.turnon();
		Timed.simulateUntilLastEvent();
		VirtualAppliance va = new VirtualAppliance("VAID", 10, 0, false,
				100000000l);
		pm.localDisk.registerObject(va);
		AlterableResourceConstraints caps = new AlterableResourceConstraints(
				pm.getCapacities());
		caps.multiply(0.4);
		ConstantConstraints minCaps = new ConstantConstraints(caps);
		ResourceAllocation ra1 = pm.allocateResources(minCaps, true, 10);
		ResourceAllocation ra2 = pm.allocateResources(minCaps, true, 10);
		VirtualMachine vm1 = new VirtualMachine(va);
		VirtualMachine vm2 = new VirtualMachine(va);
		pm.deployVM(vm1, ra1, pm.localDisk);
		pm.deployVM(vm2, ra2, pm.localDisk);
		Timed.simulateUntilLastEvent();
	}
}
