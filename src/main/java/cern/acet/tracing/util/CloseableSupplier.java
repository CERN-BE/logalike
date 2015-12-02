/**
 * Logalike - A stream based message processor
 * Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All Rights Reserved.
 * This software is distributed under the terms of the GNU General Public Licence version 3 (GPL Version 3),
 * copied verbatim in the file “COPYLEFT”.
 * In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue
 * of its status as an Intergovernmental Organization or submit itself to any jurisdiction. 
 * 
 * Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing.util;

import java.util.function.Supplier;

/**
 * A {@link Supplier} that can be handled and closed as a resource.
 * 
 * @author jepeders
 * @param <T> The type of object this supplier supplies.
 */
public interface CloseableSupplier<T> extends Supplier<T>, AutoCloseable {

    /* No implementation */

}
