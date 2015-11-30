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

import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;
import hu.unimiskolc.iit.distsys.interfaces.CloudProvider;
import hu.unimiskolc.iit.distsys.interfaces.FillInAllPMs;
import hu.unimiskolc.iit.distsys.interfaces.VMCreationApproaches;

public class TestCreatorFactory {
	public static VMCreationApproaches createApproachesExercise()
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		return (VMCreationApproaches) Class.forName(System.getProperty("hu.unimiskolc.iit.distsys.VMC")).newInstance();
	}

	public static BasicJobScheduler createARoundRobinScheduler()
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		return (BasicJobScheduler) Class.forName(System.getProperty("hu.unimiskolc.iit.distsys.RRJSched"))
				.newInstance();
	}

	public static FillInAllPMs getPMFiller()
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		return (FillInAllPMs) Class.forName(System.getProperty("hu.unimiskolc.iit.distsys.PMFiller")).newInstance();
	}

	public static CloudProvider getNewProvider()
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		return (CloudProvider) Class.forName(System.getProperty("hu.unimiskolc.iit.distsys.CustomCloudProvider"))
				.newInstance();
	}

}
