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

package cern.acet.tracing.processing.window;

import cern.acet.tracing.Message;

/**
 * A functional interface that can collect a 'fingerprint' from a {@link Message}. Depending on the situation, this
 * fingerprint can be used to find equality between two messages, if they produce the same fingerprint.
 * 
 * @param <MessageType> The type of {@link Message} to take fingerprints of.
 * @author ghoranyi, jepeders
 */
@FunctionalInterface
public interface FingerprintStrategy<MessageType extends Message<MessageType>> {

    /**
     * Retrieves the unique fingerprint from the given message.
     * 
     * @param message The {@link Message} to get the fingerprint from.
     * @return A String.
     */
    String getFingerprint(MessageType message);

}
