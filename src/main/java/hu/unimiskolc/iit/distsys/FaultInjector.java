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

import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.unimiskolc.iit.distsys.forwarders.PMForwarder;

public class FaultInjector extends Timed {
	public static boolean simulationisComplete = false;

	private ArrayList<PMDeregPreparator> myHandlers = new ArrayList<PMDeregPreparator>();
	private final IaaSService iaas;
	private final double likelyHood;

	public FaultInjector(long faultFreq, double faultLikelyHood, IaaSService iaas) {
		subscribe(faultFreq);
		this.iaas = iaas;
		likelyHood = faultLikelyHood;
	}

	@Override
	public void tick(long fires) {
		ArrayList<PhysicalMachine> pmlist = new ArrayList<PhysicalMachine>();
		for (PhysicalMachine pm : iaas.machines) {
			if (Math.random() <= likelyHood * ((PMForwarder) pm).getReliMult()) {
				pmlist.add(pm);
				for (PMDeregPreparator vmh : myHandlers) {
					// ensuring we are not going to delist a PM that is
					// already about to be delisted
					if (vmh.pm == pm) {
						pmlist.remove(pmlist.size() - 1);
					}
				}
			}
		}
		for (PhysicalMachine pm : pmlist) {
			try {
				myHandlers.add(new PMDeregPreparator(pm, new PMDeregPreparator.DeregPreparedCallback() {

					@Override
					public void deregistrationPrepared(PMDeregPreparator forPM) {
						try {
							// drops the old machine
							iaas.deregisterHost(forPM.pm);

							// adds a new host so we are not short of
							// hosts
							PhysicalMachine newPM = null;
							do {
								if (newPM != null) {
									ExercisesBase.dropPM(newPM);
								}
								newPM = ExercisesBase.getNewPhysicalMachine();
								// let's throw away those replacements
								// that are
								// having too little CPU counts for our
								// purposes
							} while (newPM.getCapacities().getRequiredCPUs() < forPM.pm.getCapacities()
									.getRequiredCPUs());
							iaas.registerHost(newPM);
							myHandlers.remove(forPM);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				}, false));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (simulationisComplete) {
			unsubscribe();
		}
	}
}
