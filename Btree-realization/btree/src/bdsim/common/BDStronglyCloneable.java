package bdsim.common;

/**
 * Cloneable doesn't contain any methods, so you can't simply have a reference
 * to a Cloneable and then call clone() on it (how annoying!). This interface
 * makes that possible. 
 * 
 * @author acath
 */
public interface BDStronglyCloneable extends Cloneable {

	public Object clone();
	
}
