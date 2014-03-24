package edu.berkeley.cs.boom.molly.report

import argonaut._, Argonaut._
import edu.berkeley.cs.boom.molly.{Run, RunStatus, FailureSpec, UltimateModel}

/**
 * Argonaut CodecJsons for converting our objects to JSON;
 * see http://argonaut.io/doc/codec/.
 */
object MollyCodecJsons {

  implicit def UltimateModelCodecJson: CodecJson[UltimateModel] =
    casecodec1(UltimateModel.apply, UltimateModel.unapply)("tables")

  implicit def RunStatusCodecJson: CodecJson[RunStatus] =
    CodecJson.derived(StringEncodeJson.contramap((x: RunStatus) => x.underlying),
                      StringDecodeJson.map(RunStatus.apply))

  implicit def RunCodecJson: CodecJson[Run] =
    casecodec4(Run.apply, Run.unapply)("iteration", "status", "failureSpec", "model")

  implicit def FailureSpecCodecJson: CodecJson[FailureSpec] =
    casecodec4(FailureSpec.apply, FailureSpec.unapply)("eot", "eff", "crashes", "nodes")

}
