/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** See the LICENSE file for license terms.
 */

package lob

import (
	"errors"
	"testing"
)

type preparedReferencesTestLocator struct{}

func (preparedReferencesTestLocator) Snapshot() []byte { return []byte{1} }
func (preparedReferencesTestLocator) MarkReleased()    {}
func (preparedReferencesTestLocator) ReferenceIdentity() (ReferenceID, error) {
	return "test", nil
}

type preparedReferencesTestToken struct{ id int }

func (*preparedReferencesTestToken) lobReferenceToken() {}

type preparedReferencesTestRegistry struct {
	retained   []Reference
	released   []Reference
	retainErr  error
	releaseErr error
}

func (registry *preparedReferencesTestRegistry) Retain(Locator) (Reference, error) {
	if registry.retainErr != nil {
		return nil, registry.retainErr
	}
	reference := &preparedReferencesTestToken{id: len(registry.retained) + 1}
	registry.retained = append(registry.retained, reference)
	return reference, nil
}

func (registry *preparedReferencesTestRegistry) Release(reference Reference) error {
	registry.released = append(registry.released, reference)
	return registry.releaseErr
}

func (*preparedReferencesTestRegistry) ReservePending() (FreeBatch, error) { return nil, nil }
func (*preparedReferencesTestRegistry) CompletePending(FreeBatch)          {}
func (*preparedReferencesTestRegistry) RestorePending(FreeBatch)           {}
func (*preparedReferencesTestRegistry) Discard()                           {}

func TestNewPreparedReferencesValidatesOwner(t *testing.T) {
	tests := []struct {
		name     string
		registry ReferenceRegistry
		onUnsafe func()
		wantErr  bool
	}{
		{name: "nil registry", onUnsafe: func() {}, wantErr: true},
		{name: "nil callback", registry: &preparedReferencesTestRegistry{}, wantErr: true},
		{name: "valid", registry: &preparedReferencesTestRegistry{}, onUnsafe: func() {}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner, err := NewPreparedReferences(test.registry, test.onUnsafe)
			if (err != nil) != test.wantErr {
				t.Fatalf("NewPreparedReferences() error = %v, wantErr %v", err, test.wantErr)
			}
			if !test.wantErr && owner == nil {
				t.Fatal("NewPreparedReferences() returned a nil owner")
			}
		})
	}
}

func TestPreparedReferencesAddAndReleaseInReverseOrder(t *testing.T) {
	registry := &preparedReferencesTestRegistry{}
	owner, err := NewPreparedReferences(registry, func() {})
	if err != nil {
		t.Fatal(err)
	}
	locator := preparedReferencesTestLocator{}
	if err := owner.Add(locator); err != nil {
		t.Fatal(err)
	}
	if err := owner.Add(locator); err != nil {
		t.Fatal(err)
	}
	existing := &preparedReferencesTestToken{id: 3}
	if err := owner.AddReference(existing); err != nil {
		t.Fatal(err)
	}
	if got := owner.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}
	if owner.Released() {
		t.Fatal("Released() = true before cleanup")
	}

	if err := owner.ReleaseAll(); err != nil {
		t.Fatal(err)
	}
	wantReleaseOrder := []Reference{existing, registry.retained[1], registry.retained[0]}
	if len(registry.released) != len(wantReleaseOrder) {
		t.Fatalf("release count = %d, want %d", len(registry.released), len(wantReleaseOrder))
	}
	for index, want := range wantReleaseOrder {
		if registry.released[index] != want {
			t.Errorf("released[%d] = %v, want %v", index, registry.released[index], want)
		}
	}
	if !owner.Released() {
		t.Fatal("Released() = false after cleanup")
	}
	if err := owner.ReleaseAll(); err != nil {
		t.Fatalf("second ReleaseAll() error = %v", err)
	}
	if got := len(registry.released); got != len(wantReleaseOrder) {
		t.Errorf("second ReleaseAll() changed release count to %d", got)
	}
}

func TestPreparedReferencesRejectsInvalidOwnersAndReferences(t *testing.T) {
	var nilOwner *PreparedReferences
	registry := &preparedReferencesTestRegistry{retainErr: errors.New("retain failed")}
	owner, err := NewPreparedReferences(registry, func() {})
	if err != nil {
		t.Fatal(err)
	}
	releasedOwner, err := NewPreparedReferences(&preparedReferencesTestRegistry{}, func() {})
	if err != nil {
		t.Fatal(err)
	}
	releasedOwner.released = true

	tests := []struct {
		name string
		call func() error
	}{
		{name: "add to nil owner", call: func() error { return nilOwner.Add(preparedReferencesTestLocator{}) }},
		{name: "add to released owner", call: func() error { return releasedOwner.Add(preparedReferencesTestLocator{}) }},
		{name: "registry retain failure", call: func() error { return owner.Add(preparedReferencesTestLocator{}) }},
		{name: "add reference to nil owner", call: func() error { return nilOwner.AddReference(&preparedReferencesTestToken{}) }},
		{name: "add reference to released owner", call: func() error { return releasedOwner.AddReference(&preparedReferencesTestToken{}) }},
		{name: "add nil reference", call: func() error { return owner.AddReference(nil) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("operation returned nil error")
			}
		})
	}
}

func TestPreparedReferencesReleaseFailureAbandonsOwner(t *testing.T) {
	releaseErr := errors.New("release failed")
	registry := &preparedReferencesTestRegistry{releaseErr: releaseErr}
	unsafeCalls := 0
	owner, err := NewPreparedReferences(registry, func() { unsafeCalls++ })
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Add(preparedReferencesTestLocator{}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Add(preparedReferencesTestLocator{}); err != nil {
		t.Fatal(err)
	}

	if err := owner.ReleaseAll(); !errors.Is(err, releaseErr) {
		t.Fatalf("ReleaseAll() error = %v, want %v", err, releaseErr)
	}
	if got := unsafeCalls; got != 1 {
		t.Errorf("unsafe callback count = %d, want 1", got)
	}
	if got := len(registry.released); got != 1 {
		t.Errorf("release count = %d, want stop after first failure", got)
	}
	if !owner.Released() {
		t.Error("Released() = false after failed cleanup")
	}
}

func TestPreparedReferencesAbandonAndNilSafeMethods(t *testing.T) {
	var nilOwner *PreparedReferences
	if err := nilOwner.ReleaseAll(); err != nil {
		t.Errorf("nil ReleaseAll() error = %v", err)
	}
	nilOwner.Abandon()
	if got := nilOwner.Len(); got != 0 {
		t.Errorf("nil Len() = %d, want 0", got)
	}
	if !nilOwner.Released() {
		t.Error("nil Released() = false, want true")
	}

	unsafeCalls := 0
	owner, err := NewPreparedReferences(&preparedReferencesTestRegistry{}, func() { unsafeCalls++ })
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.ReleaseAll(); err != nil {
		t.Fatal(err)
	}
	if owner.Released() {
		t.Fatal("empty owner became released during ReleaseAll()")
	}
	owner.Abandon()
	owner.Abandon()
	if got := unsafeCalls; got != 1 {
		t.Errorf("unsafe callback count = %d, want 1", got)
	}
	if !owner.Released() {
		t.Error("Released() = false after Abandon()")
	}
}
