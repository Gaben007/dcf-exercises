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

package hu.unimiskolc.iit.distsys;

import java.util.Properties;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;
import hu.unimiskolc.iit.distsys.forwarders.PMForwarder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestVMCreation {
	VMCreationApproaches vmc;

	@Before
	public void initVMC() throws ClassNotFoundException,
			InstantiationException, IllegalAccessException
	{
		setDependencies();
		
		Timed.resetTimed();
		ExercisesBase.reset();
		vmc = VMCreatorFactory.createApproachesExercise();
	}
	
	public void setDependencies(){
		Properties p = new Properties();
		p.setProperty("hu.unimiskolc.iit.distsys.VMC", "hu.unimiskolc.iit.distsys.VMCreation");
		
		System.setProperties(p);
	}

	@Test(timeout = 100)
	public void testFirstApproach() throws Exception {
		int beforeSize = ExercisesBase.pmforwarders.size();
		int beforeIaaSSize = ExercisesBase.iaasforwarders.size();
		vmc.directVMCreation();
		int afterSize = ExercisesBase.pmforwarders.size();
		int afterIaaSSize = ExercisesBase.iaasforwarders.size();
		Assert.assertEquals("Should have one more PMs created", beforeSize + 1,
				afterSize);
		Assert.assertEquals("Should have no extra IaaSs created",
				beforeIaaSSize, afterIaaSSize);
		PMForwarder analysedPM = ExercisesBase.pmforwarders.get(beforeSize);
		Assert.assertEquals(
				"Should have two VMs running in parallel on the new PM", 2,
				analysedPM.listVMs().size());
		Assert.assertTrue("Should have directly asked for the VMs",
				analysedPM.isReqVMCalled());
		for (VirtualMachine vm : analysedPM.listVMs()) {
			Assert.assertEquals("Both VMs should be running by now",
					VirtualMachine.State.RUNNING, vm.getState());
		}
	}

	@Test()
	public void testSecondApproach() throws Exception {
		int beforeSize = ExercisesBase.pmforwarders.size();
		vmc.twoPhaseVMCreation();
		int afterSize = ExercisesBase.pmforwarders.size();
		Assert.assertEquals("Should have one more PMs created", beforeSize + 1,
				afterSize);
		PMForwarder analysedPM = ExercisesBase.pmforwarders.get(beforeSize);
		Assert.assertEquals(
				"Should have two VMs running in parallel on the new PM", 2,
				analysedPM.listVMs().size());
		Assert.assertTrue("Should have requested the VMs in two phases",
				analysedPM.isAllocVMCalled() && analysedPM.isDeployVMCalled()
						&& !analysedPM.isReqVMCalled());
		for (VirtualMachine vm : analysedPM.listVMs()) {
			Assert.assertEquals("Both VMs should be running by now",
					VirtualMachine.State.RUNNING, vm.getState());
		}
	}

	@Test(timeout = 100)
	public void throughIaaSApproach() throws Exception {
		int beforePMSize = ExercisesBase.pmforwarders.size();
		int beforeIaaSSize = ExercisesBase.iaasforwarders.size();
		vmc.indirectVMCreation();
		int afterPMSize = ExercisesBase.pmforwarders.size();
		int afterIaaSSize = ExercisesBase.iaasforwarders.size();
		Assert.assertEquals("Should have one more PMs created",
				beforePMSize + 1, afterPMSize);
		Assert.assertEquals("Should have one an extra IaaSs created",
				beforeIaaSSize + 1, afterIaaSSize);
		PMForwarder analysedPM = ExercisesBase.pmforwarders.get(beforePMSize);
		IaaSForwarder iaas = ExercisesBase.iaasforwarders.get(beforeIaaSSize);
		Assert.assertEquals("Should have the PM registered to the IaaS",
				analysedPM, iaas.machines.get(0));
		Assert.assertEquals("Should have no other PM registered to the IaaS",
				1, iaas.machines.size());
		Assert.assertTrue("Should have asked for the VMs through the IaaS",
				!analysedPM.isReqVMCalled() && iaas.isReqVMcalled());
		for (VirtualMachine vm : iaas.listVMs()) {
			Assert.assertEquals("Both VMs should be running by now",
					VirtualMachine.State.RUNNING, vm.getState());
		}
	}

	@Test(timeout = 100)
	public void throughMigrationApproach() throws Exception {
		int beforePMSize = ExercisesBase.pmforwarders.size();
		vmc.migratedVMCreation();
		int afterPMSize = ExercisesBase.pmforwarders.size();
		Assert.assertEquals("Should have one more PMs created",
				beforePMSize + 2, afterPMSize);
		PMForwarder start = ExercisesBase.pmforwarders.get(beforePMSize);
		PMForwarder stop = ExercisesBase.pmforwarders.get(beforePMSize + 1);
		Assert.assertEquals("Should have no VMs", 0, start.listVMs().size());
		Assert.assertEquals("Should have the single VM", 1, stop.listVMs()
				.size());
		Assert.assertEquals("Should have been running a VM in the past", 1,
				start.getCompletedVMs());
		Assert.assertTrue(
				"The VM on the second PM should arrive by migration",
				stop.isAllocVMCalled() && !stop.isDeployVMCalled()
						&& !stop.isReqVMCalled());
	}
}
