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

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.JobListAnalyser;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random.RepetitiveRandomTraceGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService.IaaSHandlingException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.AlterableResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestHighAvailability {
	public static final double[] availabilityLevels = { 0.98, 0.99, 0.999};
	public static final double pmAvailability = 0.65;

	@Test(timeout = 180000)
	public void hatest() throws Exception {
		int[] successCounters = new int[availabilityLevels.length];
		int[] totalCounters = new int[availabilityLevels.length];
		final IaaSService myIaaS = ExercisesBase.getComplexInfrastructure(100);
		Repository r = myIaaS.repositories.get(0);
		VirtualAppliance va = (VirtualAppliance) r.contents().iterator().next();
		AlterableResourceConstraints totCaps = AlterableResourceConstraints.getNoResources();
		double maxNodeProcs = 0;
		for (PhysicalMachine pm : myIaaS.machines) {
			totCaps.singleAdd(pm.getCapacities());
			maxNodeProcs = Math.max(maxNodeProcs, pm.getCapacities().getRequiredCPUs());
		}
		// IaaS is prepared

		// Doing preevaluation of the infrastructure
		VirtualMachine test = myIaaS.requestVM(va, myIaaS.machines.get(0).getCapacities(), r, 1)[0];
		long preTime = Timed.getFireCount();
		Timed.simulateUntilLastEvent();
		long pastTime = Timed.getFireCount();
		long vmCreationTime = pastTime - preTime;
		test.destroy(true);
		Timed.simulateUntilLastEvent();
		Timed.resetTimed();
		// Preevaluation completed

		// Preparing the jobs for the VMs
		RepetitiveRandomTraceGenerator rrtg = new RepetitiveRandomTraceGenerator(ComplexDCFJob.class);
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
		final long lastTermination = JobListAnalyser.getLastTerminationTime(jobs) * 1000 * 2;
		for (Job j : jobs) {
			int index = RandomUtils.nextInt(0, availabilityLevels.length);
			((ComplexDCFJob) j).setAvailabilityLevel(availabilityLevels[index]);
			totalCounters[index]++;
		}
		// Joblist is ready

		// Prepares the faulty PMs
		class MyTimed extends Timed {
			public MyTimed() {
				subscribe(300000);
			}

			class VMHandler implements VirtualMachine.StateChange {
				private final PhysicalMachine pm;
				private ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();

				public VMHandler(final PhysicalMachine pm) throws SecurityException, IaaSHandlingException,
						InstantiationException, IllegalAccessException, NoSuchFieldException, VMManagementException {
					this.pm = pm;
					updateVMSet();
				}

				/**
				 * Analyzes the PM and terminates all VMs that are running on
				 * it. It also kills the PM. It does a deregistration on the PM
				 * afterwards.
				 * 
				 * @throws VMManagementException
				 * @throws SecurityException
				 * @throws IaaSHandlingException
				 * @throws InstantiationException
				 * @throws IllegalAccessException
				 * @throws NoSuchFieldException
				 */
				private void updateVMSet() throws VMManagementException, SecurityException, IaaSHandlingException,
						InstantiationException, IllegalAccessException, NoSuchFieldException {
					vms.clear();
					vms.addAll(pm.publicVms);
					boolean noSubscription = true;
					for (int i = 0; i < vms.size(); i++) {
						VirtualMachine vm = vms.get(i);
						if (!vm.getState().equals(VirtualMachine.State.RUNNING)) {
							vm.subscribeStateChange(this);
							noSubscription = false;
						} else {
							vm.destroy(true);
						}
					}
					if (noSubscription) {
						doPMReReg(pm);
					}
				}

				/**
				 * Ensures that all VMs that switch to running are terminated
				 * 
				 * @param vm
				 * @param oldState
				 * @param newState
				 */
				@Override
				public void stateChanged(VirtualMachine vm, State oldState, State newState) {
					try {
						if (newState.equals(VirtualMachine.State.RUNNING)) {
							vm.destroy(true);
						} else if (newState.equals(VirtualMachine.State.DESTROYED)) {
							vm.unsubscribeStateChange(this);
							for (VirtualMachine currvm : vms) {
								if (!currvm.getState().equals(VirtualMachine.State.DESTROYED)) {
									return;
								}
							}
							// Ensures that the PM is actually destroyed if
							// there were no new VMs created in the meantime
							updateVMSet();
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}

				private void doPMReReg(final PhysicalMachine pm) throws IaaSHandlingException, SecurityException,
						InstantiationException, IllegalAccessException, NoSuchFieldException {
					// drops the old machine
					myIaaS.deregisterHost(pm);

					// adds a new host so we are not short of hosts
					PhysicalMachine newPM = null;
					do {
						if (newPM != null) {
							ExercisesBase.dropPM(newPM);
						}
						newPM = ExercisesBase.getNewPhysicalMachine();
						// let's throw away those replacements that are
						// having too little CPU counts for our purposes
					} while (newPM.getCapacities().getRequiredCPUs() < pm.getCapacities().getRequiredCPUs());
					myIaaS.registerHost(newPM);
				}
			}

			@Override
			public void tick(long fires) {
				ArrayList<PhysicalMachine> pmlist = new ArrayList<PhysicalMachine>();
				for (PhysicalMachine pm : myIaaS.machines) {
					if (Math.random() >= pmAvailability) {
						pmlist.add(pm);
					}
				}
				for (PhysicalMachine pm : pmlist) {
					try {
						new VMHandler(pm);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				if (lastTermination < fires) {
					unsubscribe();
				}
			}
		}
		new MyTimed();
		// Faulty PM preparation complete

		// Preparing the scheduling
		new JobtoVMScheduler(myIaaS, jobs);

		Timed.simulateUntilLastEvent();

		for (final Job j : jobs) {
			ComplexDCFJob jobconv = (ComplexDCFJob) j;
			// Basic tests:
			Assert.assertTrue("All jobs should start but " + j + " did not", j.getRealqueueTime() >= 0);
			if (j.getRealstopTime() >= 0) {
				successCounters[Arrays.binarySearch(availabilityLevels, jobconv.getAvailabilityLevel())]++;
				// More complex tests:
				// Should not allow too slow execution time
				Assert.assertTrue(
						"Every job should run faster or equal than it was originally expected but " + j + " did so",
						j.getExectimeSecs() * 3 > j.getRealstopTime() - j.getRealqueueTime());
				// Should not allow too long queueing time
				Assert.assertTrue("Jobs should not queue more than a VM instantiation time but " + j + " did so",
						j.getRealqueueTime() < vmCreationTime * 3);
			}
		}

		for (int i = 0; i < availabilityLevels.length; i++) {
			System.out.println(availabilityLevels[i] + " " + successCounters[i] + " " + totalCounters[i]);
			Assert.assertEquals(
					"Jobs with availability level " + availabilityLevels[i] + " did not get their expected qualities",
					availabilityLevels[i], (double) successCounters[i] / totalCounters[i],
					(1 - availabilityLevels[i]) * 0.5);
		}
	}
}
