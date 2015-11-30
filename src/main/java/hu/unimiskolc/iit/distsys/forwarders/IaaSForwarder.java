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

package hu.unimiskolc.iit.distsys.forwarders;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.pmscheduling.PhysicalMachineController;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.vmscheduling.Scheduler;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

public class IaaSForwarder extends IaaSService implements ForwardingRecorder {
	public interface VMListener {
		void newVMadded(VirtualMachine[] vms);
	}

	public interface QuoteProvider {
		/**
		 * 
		 * @param rc
		 *            if null then the default instance price should be returned
		 * @return
		 */
		double getPerTickQuote(ResourceConstraints rc);
	}

	private boolean reqVMcalled = false;
	private VMListener notifyMe = null;
	private QuoteProvider qp = new QuoteProvider() {
		@Override
		public double getPerTickQuote(ResourceConstraints rc) {
			return 1;
		}
	};

	public IaaSForwarder(Class<? extends Scheduler> s, Class<? extends PhysicalMachineController> c)
			throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		super(s, c);
	}

	public void setVMListener(VMListener newListener) {
		notifyMe = newListener;
	}

	public void setQuoteProvider(QuoteProvider qp) {
		this.qp = qp;
	}

	public double getResourceQuote(ResourceConstraints rc) {
		return qp.getPerTickQuote(rc);
	}

	public void resetForwardingData() {
		reqVMcalled = false;
	}

	public boolean isReqVMcalled() {
		return reqVMcalled;
	}

	@Override
	public VirtualMachine[] requestVM(VirtualAppliance va, ResourceConstraints rc, Repository vaSource, int count)
			throws hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException, NetworkException {
		reqVMcalled = true;
		return notifyVMListeners(super.requestVM(va, rc, vaSource, count));
	}

	@Override
	public VirtualMachine[] requestVM(VirtualAppliance va, ResourceConstraints rc, Repository vaSource, int count,
			HashMap<String, Object> schedulingConstraints)
					throws hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException, NetworkException {
		reqVMcalled = true;
		return notifyVMListeners(super.requestVM(va, rc, vaSource, count, schedulingConstraints));
	}

	private VirtualMachine[] notifyVMListeners(VirtualMachine[] received) {
		if (notifyMe != null) {
			notifyMe.newVMadded(received);
		}
		return received;
	}
}
