Require Import ZArith.
Open Scope Z_scope.

From Hammer Require Import Tactics.

Section defn.
  Inductive QoS := QoS1 | QoS2.

  Context (epoch_size : Z) {Hepoch_size : epoch_size > 0}.

  Definition seqno_to_packet_id (q : QoS) (n : Z) : Z :=
    match q with
    | QoS1 => n mod epoch_size
    | QoS2 => n mod epoch_size + epoch_size
    end.

  Definition packet_id_to_seqno (packet_id n_comm : Z) :=
    let epoch := n_comm / epoch_size in
    let seqno_offset := packet_id mod epoch_size in
    let seqno := epoch * epoch_size + seqno_offset in
    if seqno <? n_comm then
      seqno + epoch_size
    else
      seqno.

  Local Ltac blah := auto with zarith.
  Local Ltac blahblah := sauto with zarith.

  (** Theorem: packet IDs for different QoS tracks never overlap.

      I.e. it's possible to unambigously derive QoS of a message
      by its packet ID.
   *)
  Theorem no_qos_track_overlap : forall n m,
      0 <= n -> 0 <= m ->
      seqno_to_packet_id QoS1 n <> seqno_to_packet_id QoS2 m.
  Proof with blahblah.
    intros n m Hnpos Hmpos.
    set (q1 := seqno_to_packet_id QoS1 n).
    set (q2 := seqno_to_packet_id QoS2 m).
    unfold seqno_to_packet_id in *.
    assert (Hq1 : q1 < epoch_size). {
      eapply Z.mod_pos_bound...
    }
    assert (Hq2 : epoch_size <= q2). {
      unfold q2.
      assert (0 <= m mod epoch_size). {
        destruct (Z.mod_bound_pos_le m epoch_size) as [Hm _]...
      }
      blah.
    }
    blah.
  Qed.

  Lemma modmod (a b : Z) :
    b > 0 ->
    (a mod b) mod b = a mod b.
  Proof.
    intros Hb.
    rewrite Zmod_small.
    - reflexivity.
    - apply Z.mod_pos_bound. blah.
  Qed.

  Lemma modmod_plus (a b : Z) :
    b > 0 ->
    (a mod b + b) mod b = a mod b.
  Proof with blah.
    intros.
    rewrite <-Zplus_mod_idemp_r, Z.mod_same, Z.add_0_r...
    apply modmod...
  Qed.

  Lemma mod_minus_mod_lt (a b c : Z) :
    c > 0 ->
    a mod c - b mod c < c.
  Proof with blah.
    intros Hc.
    set (a' := a mod c).
    set (b' := b mod c).
    assert (a' < c). {
      destruct (Z.mod_pos_bound a c) as [_ H]...
    }
    assert (0 <= b'). {
      destruct (Z.mod_pos_bound b c)...
    }
    replace c with (c - 0) by apply Z.sub_0_r.
    apply Z.sub_lt_le_mono...
  Qed.

  Lemma mod_minus_small1 (a b c : Z) :
    0 <= a ->
    0 <= b ->
    0 <= a - b < c ->
    0 <= a mod c - b mod c ->
    a mod c - b mod c = a - b.
  Proof with blah.
    intros Ha Hb Hab Hmab.
    rewrite <-Zmod_small with (a := (a - b)) (n := c)...
    rewrite Zminus_mod.
    rewrite Zmod_small with (a := a mod c - b mod c).
    - reflexivity.
    - split.
      + assumption.
      + apply mod_minus_mod_lt...
  Qed.

  Lemma mod_minus_small2 (a b c : Z) :
    0 <= a ->
    0 <= b ->
    0 <= a - b < c ->
    a mod c - b mod c < 0 ->
    a mod c - b mod c = a - b - c.
  Proof with blah.
    intros Ha Hb hab Hmab.
    rewrite <-Z.add_cancel_r with (p := c).
    replace (a - b - c + c) with (a - b)...
    rewrite <-Zmod_small with (a := a - b) (n := c)...
    rewrite Zminus_mod.
    rewrite <-Zmod_small with (a := a mod c - b mod c + c) (n := c).
    - rewrite <-Zplus_mod_idemp_r, Z.mod_same, Z.add_0_r...
    - split.
      + replace 0 with (-c + c)...
        apply Z.add_le_mono_r.
        rewrite Z.opp_le_mono, Z.opp_involutive.
        replace (-(a mod c - b mod c)) with (b mod c - a mod c)...
        apply Z.lt_le_incl, mod_minus_mod_lt...
      + rewrite <-Z.add_0_l.
        apply Z.add_lt_mono_r...
  Qed.

  Lemma seqno_reconstruct0 n_comm n_pack (Hcomm : 0 <= n_comm) (Hrmax : 0 <= n_pack - n_comm < epoch_size):
    (if n_comm - n_comm mod epoch_size + n_pack mod epoch_size <? n_comm
     then n_comm - n_comm mod epoch_size + n_pack mod epoch_size + epoch_size
     else n_comm - n_comm mod epoch_size + n_pack mod epoch_size) = n_pack.
  Proof with blah.
    replace (n_comm - n_comm mod epoch_size + n_pack mod epoch_size) with
            (n_comm + n_pack mod epoch_size - n_comm mod epoch_size)...
    remember (n_comm + n_pack mod epoch_size - n_comm mod epoch_size <? n_comm) as underflow.
    destruct underflow.
    - assert (n_comm + n_pack mod epoch_size - n_comm mod epoch_size < n_comm)...
      assert (n_pack mod epoch_size - n_comm mod epoch_size < 0)...
      replace (n_comm + n_pack mod epoch_size - n_comm mod epoch_size + epoch_size) with
              (n_comm + (n_pack mod epoch_size - n_comm mod epoch_size + epoch_size))...
      rewrite mod_minus_small2...
    - assert (n_comm + n_pack mod epoch_size - n_comm mod epoch_size >= n_comm)...
      assert (0 <= n_pack mod epoch_size - n_comm mod epoch_size)...
      replace (n_comm + n_pack mod epoch_size - n_comm mod epoch_size) with
              (n_comm + (n_pack mod epoch_size - n_comm mod epoch_size))...
      rewrite mod_minus_small1...
  Qed.

  (** Theorem: it is always possible to reconstruct sequence number of
  a message by the packet id and committed sequence number, as long as
  their difference is less than the epoch size. *)
  Theorem seqno_reconstruct (qos : QoS) (n_comm : Z) (n_pack : Z):
    0 <= n_comm ->
    0 <= n_pack - n_comm < epoch_size ->
    packet_id_to_seqno (seqno_to_packet_id qos n_pack) n_comm = n_pack.
  Proof with blah.
    intros Hcomm Hrmax.
    unfold packet_id_to_seqno, seqno_to_packet_id.
    replace (n_comm / epoch_size * epoch_size) with
            (n_comm - (n_comm mod epoch_size)).
    2:{ rewrite Zmod_eq... }
    destruct qos.
    - rewrite modmod...
      apply seqno_reconstruct0...
    - rewrite modmod_plus...
      apply seqno_reconstruct0...
  Qed.
End defn.
