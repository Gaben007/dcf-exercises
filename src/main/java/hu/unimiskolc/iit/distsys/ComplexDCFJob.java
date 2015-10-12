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

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.examples.jobhistoryprocessor.DCFJob;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;

import java.util.HashMap;

public class ComplexDCFJob extends DCFJob implements ConsumptionEvent {
	private static HashMap<VirtualMachine, DeferredEvent> vmMarkers = new HashMap<VirtualMachine, DeferredEvent>();
	private static int failingVMCounter = 0;
	private static int vmReuseCount = 0;
	public static final long noJobVMMaxLife = 30000;
	private boolean allowBasicOperations = false;
	private ConsumptionEvent myEvent;
	private VirtualMachine myVM;

	/**
	 * Constructs the job object (to be used by job generators)
	 * 
	 * @param id
	 * @param submit
	 * @param queue
	 * @param exec
	 * @param nprocs
	 * @param ppCpu
	 * @param ppMem
	 * @param user
	 * @param group
	 * @param executable
	 * @param preceding
	 * @param delayAfter
	 */
	public ComplexDCFJob(String id, long submit, long queue, long exec,
			int nprocs, double ppCpu, long ppMem, String user, String group,
			String executable, Job preceding, long delayAfter) {
		super(id, submit, queue, exec, nprocs, ppCpu, ppMem, user, group,
				executable, preceding, delayAfter);
	}

	/**
	 * Runs the job in the specified VM. Sends out a notification if the job is
	 * completed
	 * 
	 * @param vm
	 * @param completionEvent
	 * @throws NetworkException
	 */
	public void startNowOnVM(VirtualMachine vm, ConsumptionEvent completionEvent)
			throws NetworkException {
		if (vm.getState().equals(VirtualMachine.State.RUNNING)) {
			if (myEvent == null) {
				DeferredEvent de = vmMarkers.remove(vm);
				if (de != null) {
					de.cancel();
					vmReuseCount++;
				}
				vm.newComputeTask(getExectimeSecs() * 1000 * nprocs
						* ExercisesBase.maxProcessingCap,
						ResourceConsumption.unlimitedProcessing, this);
				allowBasicOperations = true;
				started();
				allowBasicOperations = false;
				myEvent = completionEvent;
				myVM = vm;
			}
		}
	}

	/**
	 * If the internal representation of the job is somehow cancelled we are
	 * showing that with a runtime exception!
	 */
	public void conCancelled(ResourceConsumption problematic) {
		throw new RuntimeException("CANCELLED: " + this);
	}

	/**
	 * Marks the completion of the internal representation of the job. Ensures
	 * that calling this function has no effect by anyone else but the
	 * DISSECT-CF simulator. Checks if the VM where the job ran does not have
	 * too long idle time.
	 */
	public void conComplete() {
		if (Thread.currentThread().getStackTrace()[2]
				.getClassName()
				.equals("hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceSpreader$FreqSyncer")) {
			// Only allow the operations take place if the conComplete was not
			// called directly.
			allowBasicOperations = true;
			completed();
			allowBasicOperations = false;
			// Forward the event to the one who started the task on the VM
			myEvent.conComplete();
			DeferredEvent theDE = new DeferredEvent(noJobVMMaxLife) {
				@Override
				protected void eventAction() {
					if (myVM.getState().equals(VirtualMachine.State.RUNNING)) {
						failingVMCounter++;
					}
					vmMarkers.remove(myVM);
				}
			};
			vmMarkers.put(myVM, theDE);
		}
	}

	/**
	 * Masked forwarder for the start events.
	 */
	@Override
	public void started() {
		if (allowBasicOperations) {
			super.started();
		}
	}

	/**
	 * Masked forwarder for the completion events.
	 */
	@Override
	public void completed() {
		if (allowBasicOperations) {
			super.completed();
		}
	}

	/**
	 * Returns the number of VMs that got idle for too much time
	 * 
	 * @return
	 */
	public static int getFailingVMCounter() {
		return failingVMCounter;
	}

	/**
	 * Returns the number of VMs that got reused over the simulation
	 * 
	 * @return
	 */
	public static int getVmReuseCount() {
		return vmReuseCount;
	}
}
