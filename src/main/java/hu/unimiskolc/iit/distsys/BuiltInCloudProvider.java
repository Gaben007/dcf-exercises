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

import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;
import hu.unimiskolc.iit.distsys.interfaces.CloudProvider;

public class BuiltInCloudProvider implements CloudProvider, VMManager.CapacityChangeEvent<PhysicalMachine>, IaaSForwarder.VMListener {
	IaaSService myProvidedService;
	
	public static double maxProcessor = 0;
	public static double maxPower = 0;
	public static double maxPowerPerProc = 0;
	public static double allProcessor = 0;
	public static double allRequest = 0;
	public static double requestedVmsCount = 0;
	public static double vmRequestCount = 0;
	public static double sumRequestResults = 0;
	public static double sumEffectiveness = 0;
	public static double sumProc = 0;

	@Override
	public void setIaaSService(IaaSService iaas) {
		myProvidedService = iaas;
		myProvidedService.subscribeToCapacityChanges(this);
		((IaaSForwarder) myProvidedService).setQuoteProvider(this);
		((IaaSForwarder) myProvidedService).setVMListener(this);
	}

	@Override
	public void capacityChanged(ResourceConstraints newCapacity, List<PhysicalMachine> affectedCapacity) {
		final boolean newRegistration = myProvidedService.isRegisteredHost(affectedCapacity.get(0));
		if (!newRegistration) {
			try {
				for (PhysicalMachine pm : affectedCapacity) {
					// For every lost PM we buy a new one.
					myProvidedService.registerHost(ExercisesBase.getNewPhysicalMachine(RandomUtils.nextDouble(2, 5)));
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public double getPerTickQuote(ResourceConstraints rc) {
		maxProcessor = Math.max(rc.getRequiredCPUs(), maxProcessor);
		maxPower = Math.max(rc.getTotalProcessingPower(), maxPower);
		maxPowerPerProc = Math.max(rc.getRequiredProcessingPower(), maxPowerPerProc);
		allProcessor += rc.getRequiredCPUs();
		sumProc += rc.getRequiredCPUs();
		allRequest++;
		double effectiveness = myProvidedService.getRunningCapacities().getTotalProcessingPower() / myProvidedService.getCapacities().getTotalProcessingPower();
		sumEffectiveness += effectiveness;
		
		//double result = rc.getRequiredCPUs() * 0.0000049999;
		double result = 0.002;
		
		sumRequestResults += result / rc.getRequiredCPUs();
		return result;
	}

	@Override
	public void newVMadded(VirtualMachine[] vms) {
		requestedVmsCount += vms.length;
		vmRequestCount++;
	}
}
