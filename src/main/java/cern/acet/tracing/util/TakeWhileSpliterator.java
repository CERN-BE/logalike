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

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A Spliterator that can be stopped. Heavily inspired by <a href="https://github.com/poetix/protonpack"
 * title="Protonpack on GitHub">Protonpack</a>.
 * 
 * @author jepeders
 * @param <T> The type of elements in the spliterator.
 */
class TakeWhileSpliterator<T> implements Spliterator<T> {

    private final Spliterator<T> source;
    private final Predicate<T> condition;
    private boolean conditionHolds = true;

    private TakeWhileSpliterator(Spliterator<T> source, Predicate<T> condition) {
        this.source = source;
        this.condition = condition;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        return conditionHolds && source.tryAdvance(element -> {
            conditionHolds = condition.test(element);
            if (conditionHolds) {
                action.accept(element);
            }
        });
    }

    @Override
    public int characteristics() {
        return source.characteristics() & ~Spliterator.SIZED;
    }

    @Override
    public long estimateSize() {
        return conditionHolds ? source.estimateSize() : 0;
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

    static <T> TakeWhileSpliterator<T> over(Spliterator<T> source, Predicate<T> condition) {
        return new TakeWhileSpliterator<>(source, condition);
    }
}