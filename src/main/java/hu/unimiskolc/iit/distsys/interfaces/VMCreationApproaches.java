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

package hu.unimiskolc.iit.distsys.interfaces;

public interface VMCreationApproaches {
	/**
	 * Shows how one can create a VM on a PM with a single function call.
	 * 
	 * @throws Exception
	 */
	public void directVMCreation() throws Exception;

	/**
	 * Shows how one can create a VM on a PM with resource allocations involved.
	 * 
	 * @throws Exception
	 */
	public void twoPhaseVMCreation() throws Exception;

	/**
	 * Shows how one can use an IaaS service to hide the complexity of multi PM
	 * management and still request the VM in a single call.
	 * 
	 * @throws Exception
	 */
	public void indirectVMCreation() throws Exception;

	/**
	 * Shows how a VM can be created on a PM by migrating the VM from another
	 * PM.
	 * 
	 * @throws Exception
	 */
	public void migratedVMCreation() throws Exception;
}
