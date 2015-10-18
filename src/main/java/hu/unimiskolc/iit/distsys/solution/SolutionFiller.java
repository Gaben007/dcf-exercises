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
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;
import hu.unimiskolc.iit.distsys.interfaces.FillInAllPMs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * This demonstrator class is showing how to fulfill the pm filling requirements
 * of hu.unimiskolc.iit.distsys.TestRoundRobijJobSched.
 * 
 * To use this class during the test, set the system property called
 * "hu.unimiskolc.iit.distsys.PMFiller" to
 * "hu.unimiskolc.iit.distsys.solution.SolutionFiller". This can be either
 * achieved by setting command line argument of java with the option
 * <code>-Dhu.unimiskolc.iit.distsys.PMFiller=hu.unimiskolc.iit.distsys.solution.SolutionFiller</code>
 * , or by setting the system property by calling System.setProperty() before
 * the VMCreatorFactory's getPMFiller() function is called.
 * 
 * Note: due to some bugs in DISSECT-CF, the success rate of this class is around 65%
 * 
 * @author gaborkecskemeti
 * 
 */
public class SolutionFiller implements FillInAllPMs {

	public void filler(IaaSService iaas, int vmCount) {
		// Detecting the properties of the iaas
		ResourceConstraints rcAll = iaas.getCapacities();
		long minMemory = Long.MAX_VALUE;
		double minProcessing = Double.MAX_VALUE;
		double minCores = Double.MAX_VALUE;
		Repository r = iaas.repositories.get(0);
		VirtualAppliance va = (VirtualAppliance) r.contents().iterator().next();
		for (PhysicalMachine pm : iaas.machines) {
			ResourceConstraints pmCaps = pm.getCapacities();
			minMemory = Math.min(minMemory, pmCaps.getRequiredMemory());
			minProcessing = Math.min(minProcessing,
					pmCaps.getRequiredProcessingPower());
			minCores = Math.min(minCores, pmCaps.getRequiredCPUs());

		}

		// Setting up resource constraints for the mainly uniformly spread VMs
		ConstantConstraints cc = new ConstantConstraints(
				rcAll.getRequiredCPUs() / vmCount, minProcessing, minMemory
						/ vmCount);
		try {
			// Requesting the initial vm set (bulk request!)
			iaas.requestVM(va, cc, r, vmCount - iaas.machines.size());
			Timed.simulateUntilLastEvent();
			// Determining the last remaining amount of resources for each
			// machine
			ArrayList<PhysicalMachine> sortedPMs = new ArrayList<PhysicalMachine>(
					iaas.machines);
			Comparator<PhysicalMachine> freeComp = new Comparator<PhysicalMachine>() {
				public int compare(PhysicalMachine o1, PhysicalMachine o2) {
					return (int) Math.signum(o2.freeCapacities
							.getTotalProcessingPower()
							- o1.freeCapacities.getTotalProcessingPower());
				}
			};
			Collections.sort(sortedPMs, freeComp);
			// Occupying all remaining capacities for each machine
			for (PhysicalMachine pm : sortedPMs) {
				// One by one request
				iaas.requestVM(
						va,
						new ConstantConstraints(
								// This trick is needed because of the improper
								// maintenance of the processing power for free
								// caps (a new version of dissect-cf should fix
								// it)
								pm.freeCapacities.getRequiredCPUs()
										* pm.getCapacities()
												.getRequiredProcessingPower()
										/ pm.freeCapacities
												.getRequiredProcessingPower(),
								pm.freeCapacities.getRequiredProcessingPower(),
								pm.freeCapacities.getRequiredMemory()), r, 1);
				Timed.simulateUntilLastEvent();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
