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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.JobListAnalyser;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random.RepetitiveRandomTraceGenerator;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ConsumptionEventAdapter;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;

public class MultiCloudUser extends Timed {
	public interface CompletionCallback {
		void alljobsComplete();
	}

	public static class ProviderRecord implements VirtualMachine.StateChange {
		int vmsRequested = 0;
		int vmsDestroyedbyUser = 0;

		private ArrayList<VirtualMachine> myVMSet = new ArrayList<VirtualMachine>();
		private IaaSService service;
		private VirtualAppliance va;
		private Repository repo;

		public ProviderRecord(IaaSService s) {
			service = s;
			repo = s.repositories.get(0);
			va = (VirtualAppliance) repo.contents().iterator().next();
		}

		public VirtualMachine[] getNewVM(ResourceConstraints rc, int count)
				throws VMManagementException, NetworkException {
			vmsRequested += count;
			VirtualMachine[] vms = service.requestVM(va, rc, repo, count);
			for (VirtualMachine vm : vms) {
				vm.subscribeStateChange(this);
				myVMSet.add(vm);
			}
			return vms;
		}

		public VirtualMachine getFreeVM(ResourceConstraints rc) {
			for (VirtualMachine vm : myVMSet) {
				if (isVMAccessible(vm) && vm.getResourceAllocation().allocated.compareTo(rc) >= 0) {
					return vm;
				}
			}
			return null;
		}

		@Override
		public void stateChanged(VirtualMachine vm, State oldState, State newState) {
			if (newState.equals(VirtualMachine.State.DESTROYED)) {
				myVMSet.remove(vm);
			}
		}

		public void destroyVM(VirtualMachine vm) throws VMManagementException {
			if (myVMSet.contains(vm)) {
				vm.destroy(true);
				vmsDestroyedbyUser++;
			}
		}

		public double getSuccessRatio() {
			return vmsRequested < 20 ? 1 : ((double) vmsDestroyedbyUser) / vmsRequested;
		}
	}

	private static boolean isVMAccessible(final VirtualMachine vm) {
		if (vm.getState().equals(VirtualMachine.State.RUNNING)) {
			if (vm.underProcessing.size() + vm.toBeAdded.size() == 0) {
				return true;
			}
		}
		return false;
	}

	private final ProviderRecord[] records;
	private final List<Job> jobs;
	protected int minindex = 0;
	public int successfulJobCount = 0;
	private boolean prepareForCompletion = false;
	private final CompletionCallback callback;

	public MultiCloudUser(IaaSService[] theProviders, CompletionCallback cb) throws Exception {
		callback = cb;
		records = new ProviderRecord[theProviders.length];
		for (int i = 0; i < records.length; i++) {
			records[i] = new ProviderRecord(theProviders[i]);
		}

		// Preparing the jobs for the VMs
		RepetitiveRandomTraceGenerator rrtg = new RepetitiveRandomTraceGenerator(ComplexDCFJob.class);
		// total number of jobs
		rrtg.setJobNum(RandomUtils.nextInt(100, 10000));
		// joblist properties
		rrtg.setExecmin(10);
		rrtg.setExecmax(7200);
		rrtg.setMaxgap(0);
		rrtg.setMingap(0);
		rrtg.setMaxStartSpread((int) (Constants.machineLifeTime / 1000));
		rrtg.setMaxTotalProcs(10000);
		rrtg.setMinNodeProcs(1);
		rrtg.setMaxNodeprocs(ExercisesBase.maxCoreCount);
		rrtg.setParallel(rrtg.getJobNum());
		jobs = rrtg.getAllJobs();
		Collections.sort(jobs, JobListAnalyser.startTimeComparator);
		long adjustTime = 1 + Timed.getFireCount() / 1000;
		for (Job j : jobs) {
			j.adjust(adjustTime);
			((ComplexDCFJob) j).setAvailabilityLevel(0.5);
		}
		subscribe((jobs.get(0).getSubmittimeSecs() - adjustTime) * 1000);
	}

	private void allocateVMforJob(final VirtualMachine vm, final Job toprocess) {
		try {
			((ComplexDCFJob) toprocess).startNowOnVM(vm, new ConsumptionEventAdapter() {
				@Override
				public void conComplete() {
					super.conComplete();
					successfulJobCount++;

					new DeferredEvent(Constants.anHour) {
						@Override
						protected void eventAction() {
							if (isVMAccessible(vm)) {
								boolean allComplete = true;
								for (ProviderRecord pr : records) {
									try {
										pr.destroyVM(vm);
										allComplete |= pr.myVMSet.size() == 0;
									} catch (VMManagementException e) {
										throw new RuntimeException(e);
									}
								}
								if (allComplete && prepareForCompletion) {
									callback.alljobsComplete();
								}
							}
						}
					};
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void tick(long fires) {
		for (int i = minindex; i < jobs.size(); i++) {
			final Job toprocess = jobs.get(i);
			long submittime = toprocess.getSubmittimeSecs() * 1000;
			if (minindex == jobs.size() - 3) {
				minindex += 0;
			}
			if (fires == submittime) {
				minindex++;
				ConstantConstraints cc = new ConstantConstraints(toprocess.nprocs, ExercisesBase.minProcessingCap,
						ExercisesBase.minMem / toprocess.nprocs);
				VirtualMachine vm = null;
				for (ProviderRecord pr : records) {
					vm = pr.getFreeVM(cc);
					if (vm != null)
						break;
				}
				if (vm == null) {
					ProviderRecord prBest = null;
					double priceSuccessCombined = Double.MAX_VALUE;
					for (ProviderRecord pr : records) {
						double currPriceCombo = ((IaaSForwarder) pr.service).getResourceQuote(cc)
								/ pr.getSuccessRatio();
						if (currPriceCombo < priceSuccessCombined) {
							priceSuccessCombined = currPriceCombo;
							prBest = pr;
						}
					}
					try {
						final ProviderRecord theProvider = prBest;
						final VirtualMachine theNewVM = theProvider.getNewVM(cc, 1)[0];
						theNewVM.subscribeStateChange(new VirtualMachine.StateChange() {
							@Override
							public void stateChanged(VirtualMachine vm, State oldState, State newState) {
								switch (newState) {
								case RUNNING:
									// our VM is working great!

									// we dispatch the job for it
									allocateVMforJob(theNewVM, toprocess);

									// then cancel our listener
								case DESTROYED:
								case SHUTDOWN:
								case NONSERVABLE:
									// cancel our listener
									vm.unsubscribeStateChange(this);
								default:
									// do nothing
								}
							}
						});
						// do not queue the VM forever if it is not scheduled in
						// 20 minutes then it is cancelled and the job is forgot
						// about forever
						new DeferredEvent(1200000) {
							@Override
							protected void eventAction() {
								if (theNewVM.getState().equals(VirtualMachine.State.DESTROYED)
										&& theNewVM.getTotalProcessed() == 0) {
									try {
										theProvider.service.terminateVM(theNewVM, false);
									} catch (Exception e) {
										throw new RuntimeException(e);
									}
								}
							}
						};
					} catch (VMManagementException e) {
						// The job has failed prematurely , we ignore it, but
						// the getNewVM function above reports the failure and
						// reduces the chances of choosing the particular
						// provider for a while.
					} catch (Exception e) {
						// This is unexpected behavior!
						throw new RuntimeException(e);
					}
				} else {
					// There was a VM already that is good for this job
					allocateVMforJob(vm, toprocess);
				}
			} else if (fires < submittime) {
				updateFrequency(submittime - fires);
				break;
			} else {
				minindex += 0;
			}
		}
		if (minindex == jobs.size()) {
			unsubscribe();
			prepareForCompletion = true;
		}
	}

	public int getJobTotJobCount() {
		return jobs.size();
	}

}
