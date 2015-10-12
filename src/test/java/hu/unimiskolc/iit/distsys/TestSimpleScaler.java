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

import gnu.trove.list.array.TIntArrayList;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.JobListAnalyser;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random.RepetitiveRandomTraceGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestSimpleScaler {
	@Test(timeout = 10000)
	public void testCase() throws Exception {
		// Preparing the IaaS
		final IaaSService myIaaS = ExercisesBase.getComplexInfrastructure(100);
		Repository r = myIaaS.repositories.get(0);
		VirtualAppliance va = (VirtualAppliance) r.contents().iterator().next();
		AlterableResourceConstraints totCaps = AlterableResourceConstraints
				.getNoResources();
		double maxNodeProcs = 0;
		for (PhysicalMachine pm : myIaaS.machines) {
			totCaps.singleAdd(pm.getCapacities());
			maxNodeProcs = Math.max(maxNodeProcs, pm.getCapacities()
					.getRequiredCPUs());
		}
		// IaaS is prepared

		// Doing preevaluation of the infrastructure
		VirtualMachine test = myIaaS.requestVM(va, myIaaS.machines.get(0)
				.getCapacities(), r, 1)[0];
		long preTime = Timed.getFireCount();
		Timed.simulateUntilLastEvent();
		long pastTime = Timed.getFireCount();
		long vmCreationTime = pastTime - preTime;
		test.destroy(true);
		Timed.simulateUntilLastEvent();
		Timed.resetTimed();
		// Preevaluation completed

		// Preparing the jobs for the VMs
		RepetitiveRandomTraceGenerator rrtg = new RepetitiveRandomTraceGenerator(
				ComplexDCFJob.class);
		// total number of jobs
		rrtg.setJobNum(1000);
		// joblist properties
		rrtg.setExecmin(10);
		rrtg.setExecmax(3600);
		rrtg.setMaxgap(0);
		rrtg.setMingap(0);
		rrtg.setMaxStartSpread(3600);
		rrtg.setMaxTotalProcs((int) totCaps.getRequiredCPUs());
		rrtg.setMinNodeProcs(1);
		rrtg.setMaxNodeprocs((int) maxNodeProcs);
		rrtg.setParallel(25);
		final List<Job> jobs = rrtg.getAllJobs();
		final long lastTermination = JobListAnalyser
				.getLastTerminationTime(jobs) * 1000 * 2;
		// Joblist is ready

		// Preparing the runtime checks
		final TIntArrayList vmCounts = new TIntArrayList();

		class MyTimed extends Timed {
			public MyTimed() {
				subscribe(5000);
			}

			@Override
			public void tick(long fires) {
				vmCounts.add(myIaaS.listVMs().size());
				if (lastTermination < fires) {
					unsubscribe();
				}
			}
		}
		new MyTimed();
		// Runtime checks prepared

		// Preparing the scheduling
		new JobtoVMScheduler(myIaaS, jobs);

		Timed.simulateUntilLastEvent();
		for (final Job j : jobs) {
			// Basic tests:
			Assert.assertTrue("All jobs should start but " + j + " did not",
					j.getRealqueueTime() >= 0);
			Assert.assertTrue("All jobs should be complete but " + j
					+ " did not", j.getRealstopTime() >= 0);

			// More complex tests:
			// Should not allow too slow execution time
			Assert.assertTrue(
					"Every job should run faster or equal than it was originally expected but "
							+ j + " did so",
					j.getExectimeSecs() * 1.5 > j.getRealstopTime()
							- j.getRealqueueTime());
			// Should not allow too long queueing time
			Assert.assertTrue(
					"Jobs should not queue more than a VM instantiation time but "
							+ j + " did so",
					j.getRealqueueTime() < vmCreationTime * 1.5);
		}

		boolean didDecrease = false;
		boolean didIncrease = false;
		for (int i = 1; i < vmCounts.size() && !(didIncrease && didDecrease); i++) {
			didDecrease |= vmCounts.getQuick(i) < vmCounts.getQuick(i - 1);
			didIncrease |= vmCounts.getQuick(i) > vmCounts.getQuick(i - 1);
		}

		Assert.assertTrue(
				"Should have an increasing VM count somewhere along the process",
				didIncrease);
		Assert.assertTrue(
				"Should have a decreasing VM count somewhere along the process",
				didDecrease);
		Assert.assertEquals(
				"Should not have any VMs that are actually running for more than a few seconds without a job",
				0, ComplexDCFJob.getFailingVMCounter());
		Assert.assertTrue(
				"Should have at least a few VMs that are reused for another job",
				25 < ComplexDCFJob.getVmReuseCount());
	}
}
