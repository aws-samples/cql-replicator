/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */
package com.amazon.aws.cqlreplicator.storage;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import kotlin.jvm.functions.Function0;
import org.partiql.lang.CompilerPipeline;
import org.partiql.lang.eval.Bindings;
import org.partiql.lang.eval.EvaluationSession;
import org.partiql.lang.eval.ExprValue;
import org.partiql.lang.eval.Expression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Provides intermediate JSON transformation via PartiQL */
public class IonEngine {

  private IonSystem ion;
  private CompilerPipeline pipeline;

  public IonEngine() {
    // Initializes the ion system used by PartiQL
    this.ion = IonSystemBuilder.standard().build();
    // CompilerPipeline is the main entry point for the PartiQL lib giving you access to the
    // compiler
    // and value factories
    this.pipeline = CompilerPipeline.standard(ion);
  }

  // Query originalData based on SQL-92 syntax
  public String query(String sql, String originalData) {

    Expression selectAndFilter = pipeline.compile(sql);
    try (IonReader ionReader = IonReaderBuilder.standard().build(originalData);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        IonWriter resultWriter = IonTextWriterBuilder.json().build(byteArrayOutputStream)) {
      IonDatagram values = ion.getLoader().load(ionReader);
      Function0<ExprValue> res = () -> pipeline.getValueFactory().newFromIonValue(values);

      EvaluationSession session =
          EvaluationSession.builder()
              .globals(
                  Bindings.<ExprValue>lazyBindingsBuilder()
                      .addBinding("inputDocument", res)
                      .build())
              .build();
      ExprValue selectAndFilterResult = selectAndFilter.eval(session);

      selectAndFilterResult.getIonValue().writeTo(resultWriter);
      return byteArrayOutputStream.toString();
    } catch (IOException e) {
      System.err.printf("Unable to transform %s%n", e);
    }
    return sql;
  }
}
