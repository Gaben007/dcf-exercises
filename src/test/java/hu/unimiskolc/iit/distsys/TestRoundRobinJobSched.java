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

import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.DCFJob;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random.RepetitiveRandomTraceGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.unimiskolc.iit.distsys.interfaces.FillInAllPMs;

public class TestRoundRobinJobSched {
	@Test(timeout = 10000)
	public void testRRWithPMFiller() throws Exception {
		final int requestedVMcount = 100;
		// Preparing the IaaS
		IaaSService myIaaS = ExercisesBase.getComplexInfrastructure(10);
		FillInAllPMs myPMFiller = TestCreatorFactory.getPMFiller();
		myPMFiller.filler(myIaaS, requestedVMcount);
		Assert.assertEquals("Should have all PMs running as all should be occupied with VMs", myIaaS.machines.size(),
				myIaaS.runningMachines.size());
		for (PhysicalMachine pm : myIaaS.machines) {
			Assert.assertEquals("No PM should have free CPUs", 0, pm.freeCapacities.getRequiredCPUs(), 0.00000001);
		}
		final Collection<VirtualMachine> vms = myIaaS.listVMs();
		Assert.assertEquals("Should have exactly the specified number of VMs on the IaaS", requestedVMcount,
				vms.size());
		AlterableResourceConstraints totCaps = AlterableResourceConstraints.getNoResources();
		for (VirtualMachine vm : vms) {
			Assert.assertEquals("All VMs should be running", VirtualMachine.State.RUNNING, vm.getState());
			totCaps.singleAdd(vm.getResourceAllocation().allocated);
		}
		// IaaS is prepared

		// Preparing the jobs for the VMs
		RepetitiveRandomTraceGenerator rrtg = new RepetitiveRandomTraceGenerator(DCFJob.class);
		rrtg.setExecmin(10);
		rrtg.setExecmax(3600);
		rrtg.setJobNum(1000);
		rrtg.setMaxgap(0);
		rrtg.setMingap(0);
		rrtg.setMaxStartSpread(100);
		rrtg.setMaxTotalProcs((int) totCaps.getRequiredCPUs());
		rrtg.setMinNodeProcs(1);
		rrtg.setMaxNodeprocs(1);
		rrtg.setParallel(100);
		final List<Job> jobs = rrtg.getAllJobs();
		// Joblist is ready

		// Preparing the scheduling
		new JobtoVMScheduler(vms, jobs);
		
		// Preparing the runtime checks
		class MyTimed extends Timed {
			int maxTotal;

			public MyTimed() {
				subscribe(300000);
			}

			@Override
			public void tick(long fires) {
				for (VirtualMachine vm : vms) {
					maxTotal = Math.max(maxTotal, vm.underProcessing.size());
					Assert.assertTrue("The number of processed tasks should always be below 2", maxTotal < 2);
				}
				boolean terminate = true;
				for (Job j : jobs) {
					if (j.getRealstopTime() == -1) {
						terminate = false;
					}
				}
				if (terminate) {
					unsubscribe();
				}
			}
		}
		new MyTimed();
		
		//doing the actual simulation
		Timed.simulateUntilLastEvent();

		// post runtime checks
		for (final Job j : jobs) {
			Assert.assertTrue("One of the jobs did not get started", j.getRealqueueTime() > 0);
			Assert.assertTrue("One of the jobs did not get completed", j.getRealstopTime() > 0);
		}

	}
}
